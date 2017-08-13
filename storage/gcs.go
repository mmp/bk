// storage/gcs.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package storage

import (
	"bytes"
	gcs "cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

// There are two reasons to keep this relatively low: we don't do GCS
// resumable uploads (not supported in the golang GCS library), and we also
// buffer a copy of the file contents as they're written in memory so that
// we can retry from scratch for failures.
const maxGCSPackSize = 512 * 1024 * 1024

// Implements the FileStorage interface to store files in Google Cloud
// Storage.
type gcsFileStorage struct {
	ctx    context.Context
	client *gcs.Client
	bucket *gcs.BucketHandle
}

type GCSOptions struct {
	BucketName string
	ProjectId  string
	// Optional. Will use "us-central1" if not specified.
	Location string

	// zero -> unlimited
	MaxUploadBytesPerSecond   int
	MaxDownloadBytesPerSecond int
}

func NewGCS(options GCSOptions) Backend {
	g := &gcsFileStorage{ctx: context.Background()}

	var err error
	g.client, err = gcs.NewClient(g.ctx)
	log.CheckError(err)

	// Create the bucket if it doesn't exist.
	g.bucket = g.client.Bucket(options.BucketName)
	if _, err := g.bucket.Attrs(g.ctx); err == gcs.ErrBucketNotExist {
		loc := options.Location
		if loc == "" {
			loc = "us-central1"
		}
		log.Verbose("%s: creating bucket @ %s", options.BucketName, loc)
		log.Check(options.ProjectId != "")
		err := g.bucket.Create(g.ctx, options.ProjectId,
			&gcs.BucketAttrs{Location: loc})
		log.CheckError(err)
	} else {
		log.CheckError(err)
	}

	if options.MaxUploadBytesPerSecond > 0 ||
		options.MaxDownloadBytesPerSecond > 0 {
		InitBandwidthLimit(options.MaxUploadBytesPerSecond,
			options.MaxDownloadBytesPerSecond)
	}

	return newPackFileBackend(g, maxGCSPackSize)
}

func (g *gcsFileStorage) ForFiles(prefix string, f func(n string, created time.Time)) {
	it := g.bucket.Objects(g.ctx, &gcs.Query{Prefix: prefix})
	for {
		obj, err := it.Next()
		if err == iterator.Done {
			return
		}
		log.CheckError(err)

		f(obj.Name, obj.Created)
	}
}

func (g *gcsFileStorage) String() string {
	attrs, err := g.bucket.Attrs(g.ctx)
	log.CheckError(err)
	return "gs://" + attrs.Name
}

func (g *gcsFileStorage) Fsck() bool {
	// The Fsck implementation in packFileBackend reads all the blobs
	// (twice :-( ), which can quickly get fairly expensive with GCS
	// coldline storage. (~$5 for a 40GB backup, I believe).  Therefore,
	// make sure the user really wants todo this (and have an incomplete
	// error message to hopefully force reading this comment before setting
	// the environment variable).
	if os.Getenv("BK_GCS_FSCK") != "yolo" {
		log.Fatal("Must set BK_GCS_FSCK environment variable appropriately to fsck GCS.")
	}
	return true
}

func (g *gcsFileStorage) ReadFile(name string, offset, length int64) ([]byte, error) {
	log.Debug("%s: starting gcs download, offset %d, length %d", name, offset, length)

	obj := g.bucket.Object(name)
	var b []byte
	err := retry(name, func() error {
		var r io.ReadCloser
		var err error
		if length > 0 {
			r, err = obj.NewRangeReader(g.ctx, offset, length)
		} else {
			r, err = obj.NewReader(g.ctx)
		}

		if err != nil {
			return err
		}

		b, err = ioutil.ReadAll(NewLimitedDownloadReader(r))
		r.Close()
		return err
	})
	return b, err
}

func retry(n string, f func() error) error {
	const maxTries = 5
	for tries := 0; ; tries++ {
		err := f()

		if err == nil || tries == maxTries {
			return err
		}

		// Possibly temporary error; sleep and retry.
		log.Warning("%s: sleeping due to error %s", n, err.Error())
		time.Sleep(time.Duration(100*(tries+1)) * time.Millisecond)
	}
}

func (g *gcsFileStorage) CreateFile(name string) RobustWriteCloser {
	// It seems that using Object.If(storage.Conditions{DoesNotExist:true})
	// ends up uploading the entire file contents before catching the "oh,
	// it already exists" error upon the Close() call.  Good times.
	// Checking for existence by grabbing the attrs is much more efficient.
	if _, err := g.bucket.Object(name).Attrs(g.ctx); err == nil {
		log.Fatal("%s: already exsits.", name)
	}

	storageClass := "regional"
	if strings.HasPrefix(name, "packs/") {
		storageClass = "coldline"
	}

	return &gcsWriter{
		name:         name,
		storageClass: storageClass,
		g:            g,
	}
}

// gcsWriter implements RobustWriteCloser.  It buffers the entire contents
// of the file before actually doing the upload to GCS in its Close()
// method. (This makes it easy to retry on temporary failures.)
type gcsWriter struct {
	buf          bytes.Buffer
	name         string
	storageClass string
	g            *gcsFileStorage
}

func (gw *gcsWriter) Write(b []byte) {
	// bytes.Buffer.Write is documented to never return an error (it panics
	// on OOM).
	_, _ = gw.buf.Write(b)
}

func (gw *gcsWriter) Close() {
	err := retry(gw.name, func() error {
		return gw.g.upload(gw.name, gw.storageClass, gw.buf.Bytes())
	})
	log.CheckError(err, "%s: %s", gw.name, err)
}

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

func (g *gcsFileStorage) upload(name string, storageClass string, buf []byte) error {
	// Make sure the files don't already exist. (Ideally would check this
	// before Close, but this shouldn't happen in general...)
	obj := g.bucket.Object(name)
	if _, err := obj.Attrs(g.ctx); err == nil {
		log.Fatal("%s: already exsits.", name)
	}

	tmpName := name + ".tmp"
	tmpObj := g.bucket.Object(tmpName)
	if _, err := tmpObj.Attrs(g.ctx); err == nil {
		log.Fatal("%s: already exsits.", tmpName)
	}

	log.Verbose("%s: starting upload", name)

	w := tmpObj.NewWriter(g.ctx)
	// Make it upload along the way rather than waiting until the rate
	// limiting code eventually gives it all the data.
	w.ChunkSize = 256 * 1024
	defer tmpObj.Delete(g.ctx)

	r := NewLimitedUploadReader(bytes.NewReader(buf))
	if _, err := io.Copy(w, r); err != nil {
		w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}

	log.Verbose("%s: finished upload", name)

	// Double-check that the CRC we compute locally is the same as what GCS
	// thinks it is.
	localCrc := crc32.Checksum(buf, castagnoliTable)
	gcsCrc := w.Attrs().CRC32C
	if localCrc != gcsCrc {
		// TODO: we could return an error in this case under the theory
		// that the data was most likely corrupted over the network during
		// the upload.  The other possibility is that a bit flip corrupted
		// it in local memory, so maybe best to just always fail hard?
		log.Fatal("%s: CRC32 checksum mismatch. Local: %d, GCS: %d", tmpName,
			localCrc, gcsCrc)
	}

	// Make the final object by copying from the temporary one.
	copier := obj.CopierFrom(tmpObj)
	copier.StorageClass = storageClass
	// No idea why it insists this be set directly for the copier to work.
	copier.ContentType = "application/octet-stream"

	_, err := copier.Run(g.ctx)
	return err
}
