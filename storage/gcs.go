// storage/gcs.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

// Implementation of Backend interface using the Google Cloud Storage APIs.

package storage

import (
	"bytes"
	"cloud.google.com/go/storage"
	u "github.com/mmp/bk/util"
	"golang.org/x/net/context"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"
)

// TODO: if we used resumable uploads, it might be reasonable to increase
// this size, but OTOH it's not clear that that complexity is worthwhile.
const MaxGCSPackSize = 128 * 1024 * 1024

type gcsBackend struct {
	ctx    context.Context
	client *storage.Client
	bucket *storage.BucketHandle
	start  time.Time

	metadataNames map[string]time.Time
	chunkIndex    ChunkIndex

	blobPacker        BlobPacker
	pack, idx         []byte
	packName, idxName string
	toUpload          chan toUpload
	wg                sync.WaitGroup

	// Protects the statistics variables.
	mu                             sync.Mutex
	bytesUploaded, bytesDownloaded int64
	numUploads, numDownloads       int
}

type toUpload struct {
	name         string
	storageClass string
	data         []byte
}

type GCSOptions struct {
	BucketName string
	ProjectId  string

	// zero -> unlimited
	MaxUploadBytesPerSecond   int
	MaxDownloadBytesPerSecond int
}

func NewGCS(options GCSOptions) Backend {
	log.Check(options.ProjectId != "")

	gcs := &gcsBackend{
		ctx:   context.Background(),
		start: time.Now(),
		// Limit the size so we don't use too much RAM.
		toUpload: make(chan toUpload, 2),
	}

	var err error
	gcs.client, err = storage.NewClient(gcs.ctx)
	log.CheckError(err)

	// Create the bucket if it doesn't exist.
	gcs.bucket = gcs.client.Bucket(options.BucketName)
	if _, err := gcs.bucket.Attrs(gcs.ctx); err == storage.ErrBucketNotExist {
		log.Verbose("%s: creating bucket", options.BucketName)
		bucketAttrs := storage.BucketAttrs{
			// TODO: allow specifying the Location via GCSOptions.
			Location: "us-central1",
		}
		err := gcs.bucket.Create(gcs.ctx, options.ProjectId, &bucketAttrs)
		log.CheckError(err)
	} else {
		log.CheckError(err)
	}

	if options.MaxUploadBytesPerSecond > 0 ||
		options.MaxDownloadBytesPerSecond > 0 {
		InitBandwidthLimit(options.MaxUploadBytesPerSecond,
			options.MaxDownloadBytesPerSecond)
	}

	// Query GCS for the names of the metadata.
	gcs.metadataNames = make(map[string]time.Time)
	gcs.forObjects("metadata/", func(n string, obj *storage.ObjectAttrs) {
		gcs.metadataNames[n] = obj.Created
	})

	// TODO: do in parallel?
	log.Verbose("Starting download of indices")
	gcs.forObjects("indices/", func(n string, obj *storage.ObjectAttrs) {
		b, err := gcs.download(obj.Name, 0, 0)
		log.Verbose("%s: got %d-length index file.", obj.Name, len(b))
		log.CheckError(err)
		pn := "packs/" + strings.TrimSuffix(n, ".idx") + ".pack"
		gcs.chunkIndex.AddIndexFile(pn, b)
	})
	log.Verbose("Done downloading indices.")

	gcs.wg.Add(1)
	go gcs.uploadWorker()

	return gcs
}

// Utility function that calls the provided function with each object in
// the bucket whose name has the given prefix.
func (gcs *gcsBackend) forObjects(prefix string, f func(n string, obj *storage.ObjectAttrs)) {
	it := gcs.bucket.Objects(gcs.ctx, &storage.Query{Prefix: prefix})
	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}
		log.CheckError(err)
		f(strings.TrimPrefix(obj.Name, prefix), obj)
	}
}

func (gcs *gcsBackend) String() string {
	attrs, err := gcs.bucket.Attrs(gcs.ctx)
	log.CheckError(err)
	return "gs://" + attrs.Name
}

func (gcs *gcsBackend) LogStats() {
	delta := time.Now().Sub(gcs.start)
	if gcs.numUploads > 0 {
		upBytesPerSec := float64(gcs.bytesUploaded) / delta.Seconds()
		log.Print("uploaded %s bytes in %d uploads (avg %s, %s/s)",
			u.FmtBytes(gcs.bytesUploaded), gcs.numUploads,
			u.FmtBytes(gcs.bytesUploaded/int64(gcs.numUploads)),
			u.FmtBytes(int64(upBytesPerSec)))
	}
	if gcs.numDownloads > 0 {
		downBytesPerSec := float64(gcs.bytesDownloaded) / delta.Seconds()
		log.Print("downloaded %s bytes in %d downloads (avg %s, %s/s)",
			u.FmtBytes(gcs.bytesDownloaded), gcs.numDownloads,
			u.FmtBytes(gcs.bytesDownloaded/int64(gcs.numDownloads)),
			u.FmtBytes(int64(downBytesPerSec)))
	}
}

func (gcs *gcsBackend) Write(chunk []byte) Hash {
	hash := HashBytes(chunk)
	if _, err := gcs.chunkIndex.Lookup(hash); err == nil {
		return hash
	}

	log.Debug("%s: got Write", hash)

	// Extra slop for magic number, etc.
	if len(gcs.pack)+len(chunk)+16 > MaxGCSPackSize {
		gcs.flushUploads()
	}

	if gcs.packName == "" {
		gcs.packName = "packs/" + hash.String() + ".pack"
		gcs.idxName = "indices/" + hash.String() + ".idx"
	}

	idx, pack := gcs.blobPacker.Pack(hash, chunk)

	gcs.chunkIndex.AddSingle(hash, gcs.packName, int64(len(gcs.pack)), int64(len(pack)))

	gcs.idx = append(gcs.idx, idx...)
	gcs.pack = append(gcs.pack, pack...)

	return hash
}

func (gcs *gcsBackend) uploadWorker() {
	for {
		up, ok := <-gcs.toUpload
		if !ok {
			gcs.wg.Done()
			return
		}

		gcs.upload(up.name, up.storageClass, up.data)
	}
}

func (gcs *gcsBackend) flushUploads() {
	if len(gcs.pack) == 0 {
		return
	}

	// Send off what we've got so far.  Important: first the pack file!
	gcs.toUpload <- toUpload{gcs.packName, "coldline", gcs.pack}
	gcs.toUpload <- toUpload{gcs.idxName, "regional", gcs.idx}

	// And get ready to start a new pack/index file.
	gcs.idx = nil
	gcs.pack = nil
	gcs.blobPacker = BlobPacker{}
	gcs.packName = ""
	gcs.idxName = ""
}

func (gcs *gcsBackend) SyncWrites() {
	gcs.flushUploads()

	// Shutting down the writers ensures that all writes have landed in
	// GCS.
	if gcs.toUpload != nil {
		close(gcs.toUpload)
		gcs.wg.Wait()
		gcs.toUpload = nil
	}

	// Launch a new upload worker.
	gcs.toUpload = make(chan toUpload, 2)
	gcs.wg.Add(1)
	go gcs.uploadWorker()
}

func (gcs *gcsBackend) Read(hash Hash) (io.ReadCloser, error) {
	if loc, err := gcs.chunkIndex.Lookup(hash); err != nil {
		return nil, err
	} else {
		blob, err := gcs.download(loc.PackName, loc.Offset, loc.Length)
		if err != nil {
			return nil, err
		}

		data, err := DecodeBlob(blob)
		if err != nil {
			return nil, err
		}
		log.Check(HashBytes(data) == hash)

		return ioutil.NopCloser(bytes.NewReader(data)), nil
	}
}

func (gcs *gcsBackend) HashExists(hash Hash) bool {
	_, err := gcs.chunkIndex.Lookup(hash)
	return err == nil
}

func (gcs *gcsBackend) Hashes() map[Hash]struct{} {
	return gcs.chunkIndex.Hashes()
}

func (gcs *gcsBackend) Fsck() {
	// Fsck reads all the blobs (twice :-( ), which can quickly get fairly
	// expensive with GCS coldline storage. (~$35 for a 40GB backup, I
	// believe).  Therefore, make sure the user really wants todo this (and
	// have an incomplete error message to hopefully force reading this
	// comment).
	if os.Getenv("BK_GCS_FSCK") != "yolo" {
		log.Fatal("Must set BK_GCS_FSCK environment variable appropriately to fsck GCS.")
	}

	// Mostly copied from disk.Fsck(), but it's not clear if there's a
	// reasonable / worthwhile refactoring.

	// Make sure each blob is available in a pack file and that its data's
	// hash matches the stored hash.
	allHashes := gcs.chunkIndex.Hashes()
	log.Verbose("Checking the availability and integrity of %d blobs.", len(allHashes))
	for hash := range allHashes {
		fsckHash(hash, gcs)
	}

	// Go through all of the pack files and make sure all blobs are present
	// in an index.
	gcs.forObjects("packs/", func(n string, attrs *storage.ObjectAttrs) {
		obj := gcs.bucket.Object(attrs.Name)
		r, err := obj.NewReader(gcs.ctx)
		if err != nil {
			log.Error("%s: %s", n, err)
		}
		rc := NewLimitedDownloadReader(r)
		fsckPackFile(rc, allHashes)
		rc.Close()
	})
}

func (gcs *gcsBackend) WriteMetadata(name string, data []byte) {
	if _, ok := gcs.metadataNames[name]; ok {
		log.Fatal("%s: metadata already exists", name)
	}

	// Since writes are over the network, the next time we run the reported
	// creation time will be different. Presumably that's fine.
	gcs.metadataNames[name] = time.Now()
	gcs.upload("metadata/"+name, "NEARLINE", data)
}

func (gcs *gcsBackend) ReadMetadata(name string) []byte {
	b, err := gcs.download("metadata/"+name, 0, 0)
	log.CheckError(err)
	return b
}

func (gcs *gcsBackend) ListMetadata() map[string]time.Time {
	return gcs.metadataNames
}

func (gcs *gcsBackend) download(name string, offset, length int64) ([]byte, error) {
	log.Debug("%s: starting gcs download", name)

	obj := gcs.bucket.Object(name)
	for tries := 0; ; tries++ {
		var r io.ReadCloser
		var err error
		if length > 0 {
			r, err = obj.NewRangeReader(gcs.ctx, offset, length)
		} else {
			r, err = obj.NewReader(gcs.ctx)
		}

		if err == nil {
			lr := NewLimitedDownloadReader(r)
			b, err := ioutil.ReadAll(lr)
			log.CheckError(r.Close())

			if err == nil {
				gcs.mu.Lock()
				gcs.numDownloads++
				gcs.bytesDownloaded += int64(len(b))
				gcs.mu.Unlock()

				return b, nil
			}
		}
		if tries == 4 {
			return nil, err
		}
		log.Debug("%s: sleeping due to error %s", name, err.Error())
		time.Sleep(time.Duration(100*(tries+1)) * time.Millisecond)
	}
}

func (gcs *gcsBackend) MetadataExists(name string) bool {
	_, ok := gcs.metadataNames[name]
	return ok
}

func (gcs *gcsBackend) upload(name string, storageClass string, data []byte) {
	// It seems that using Object.If(storage.Conditions{DoesNotExist:true})
	// ends up uploading the entire file contents before catching the "oh,
	// it already exists" error upon the Close() call.  Good
	// times. Checking for existence by grabbing the attrs uses much less
	// bandwidth.
	if _, err := gcs.bucket.Object(name).Attrs(gcs.ctx); err == nil {
		log.Fatal("%s: already exsits.", name)
	}

	for tries := 0; ; tries++ {
		log.Debug("%s: starting upload. Try %d.", name, tries)

		// Upload the contents of data to the GCS object.
		w := gcs.NewWriter(name, storageClass)
		r := NewLimitedUploadReader(ioutil.NopCloser(bytes.NewBuffer(data)))
		defer r.Close()
		var err error
		if _, err = io.Copy(w, r); err == nil {
			log.Debug("copy ok")
			if err = w.Close(); err == nil {
				log.Debug("close ok")
				// Success!
				gcs.mu.Lock()
				gcs.bytesUploaded += int64(len(data))
				gcs.numUploads++
				gcs.mu.Unlock()
				return
			} else {
				log.Debug("close err %s", err)
			}
		} else {
			w.Close()
		}

		if err, ok := err.(*googleapi.Error); ok {
			switch err.Code {
			case 503:
				// Service unavailable; try again...
				// TODO: are there other 500-style errors to look out for?
				log.Debug("%s: sleeping due to error %s", name, err.Error())
				time.Sleep(time.Duration(100*(tries+1)) * time.Millisecond)
			default:
				log.CheckError(err)
			}
		} else {
			log.CheckError(err)
		}
	}
	log.Fatal("%s: upload failed after multiple tries", name)
}

///////////////////////////////////////////////////////////////////////////
// robustGcsWriter

type robustGcsWriter struct {
	gcs          *gcsBackend
	w            *storage.Writer
	tmpObj       *storage.ObjectHandle
	name         string
	storageClass string
}

func (gcs *gcsBackend) NewWriter(name string, storageClass string) *robustGcsWriter {
	w := &robustGcsWriter{
		gcs:          gcs,
		name:         name,
		tmpObj:       gcs.bucket.Object(name + ".tmp"),
		storageClass: storageClass,
	}

	if _, err := w.tmpObj.Attrs(gcs.ctx); err == nil {
		log.Fatal("%s: already exsits.", name+".tmp")
	}
	if _, err := gcs.bucket.Object(name).Attrs(gcs.ctx); err == nil {
		log.Fatal("%s: already exsits.", name)
	}

	w.w = w.tmpObj.NewWriter(gcs.ctx)

	return w
}

func (w *robustGcsWriter) Write(b []byte) (int, error) {
	return w.w.Write(b)
}

func (w *robustGcsWriter) Close() error {
	// No matter what, make sure we get rid of the temp file.
	defer w.tmpObj.Delete(w.gcs.ctx)

	// Make sure the bits are safely in storage.
	if err := w.w.Close(); err != nil {
		return err
	}

	// Make the final object and copy from the temporary one.
	obj := w.gcs.bucket.Object(w.name)
	copier := obj.CopierFrom(w.tmpObj)
	copier.StorageClass = w.storageClass
	// No idea why it insists this be set directly for the copier to work.
	copier.ContentType = "application/octet-stream"

	_, err := copier.Run(w.gcs.ctx)
	return err
}
