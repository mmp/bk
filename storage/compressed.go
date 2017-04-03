// storage/compressed.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package storage

import (
	"bytes"
	"compress/gzip"
	u "github.com/mmp/bk/util"
	"io"
	"os"
	"sync"
	"time"
)

///////////////////////////////////////////////////////////////////////////
// compressed

// compressed implements the Backend interface. It applies gzip compression
// to the provided data before passing it along to another backend for
// storage.
type compressed struct {
	backend                            Backend
	bytesSaved, bytesProcessed         int64
	compressedBlobs, uncompressedBlobs int
}

// NewCompressed returns a new storage.Backend that applies gzip compression
// to the contents of blobs stored in the provided underlying backend.
// Note: the contents of metadata files are not compressed.
func NewCompressed(backend Backend) Backend {
	return &compressed{backend: backend}
}

func (c *compressed) String() string {
	return "gzip compressed " + c.backend.String()
}

func (c *compressed) LogStats() {
	tot := c.compressedBlobs + c.uncompressedBlobs
	if tot > 0 {
		log.Print("compressed %d / %d blobs (%2.f%%)",
			c.compressedBlobs, tot, 100.*float64(c.compressedBlobs)/float64(tot))
		log.Print("passed through %s / %s input bytes (%.2f%%)",
			u.FmtBytes(c.bytesSaved), u.FmtBytes(c.bytesProcessed),
			100.*float64(c.bytesSaved)/float64(c.bytesProcessed))
	}
	c.backend.LogStats()
}

func (c *compressed) Fsck() {
	c.backend.Fsck()
}

// Reusing gzip writers gives a huge benefit; an almost 40% reduction in
// overall runtime thanks to much less GC.
var writerPool = sync.Pool{
	New: func() interface{} {
		return gzip.NewWriter(os.Stderr)
	},
}

func (c *compressed) Write(data []byte) Hash {
	// Compress the input to a buffer.
	var compressed bytes.Buffer

	//w := gzip.NewWriter(&compressed)
	w := writerPool.Get().(*gzip.Writer)
	w.Reset(&compressed)
	defer writerPool.Put(w)

	_, err := w.Write(data)
	log.CheckError(err)
	err = w.Close()
	log.CheckError(err)

	c.bytesProcessed += int64(len(data))

	// Is the compressed buffer smaller than the input?
	var stored []byte
	if compressed.Len() < len(data) {
		// Yes; write out a 1 byte to indicate that the rest of the blob is
		// indeed compressed and then save the compressed bytes.
		stored = append([]byte{1}, compressed.Bytes()...)
		c.compressedBlobs++
	} else {
		// No; write a 0 to indicate that the data is uncompressed before
		// storing the original data.
		stored = append([]byte{0}, data...)
		c.uncompressedBlobs++
	}
	c.bytesSaved += int64(len(stored))
	return c.backend.Write(stored)
}

func (c *compressed) SyncWrites() {
	c.backend.SyncWrites()
}

func (c *compressed) HashExists(hash Hash) bool {
	return c.backend.HashExists(hash)
}

func (c *compressed) Hashes() map[Hash]struct{} {
	return c.backend.Hashes()
}

// Reusing readers gives a smaller benefit than writers, but still ~15%.
var readerPool = sync.Pool{
	New: func() interface{} {
		// "foo", gzip compressed, to give us a valid initial reader
		// without an error being issued.  (Its state will be reset
		// immediately after it's fetched from the pool.)
		foo := []byte{0x1f, 0x8b, 0x8, 0x0, 0x0, 0x9, 0x6e, 0x88, 0x0, 0xff}
		r, err := gzip.NewReader(bytes.NewReader(foo))
		log.CheckError(err)
		return r
	},
}

func (c *compressed) Read(hash Hash) (io.ReadCloser, error) {
	r, err := c.backend.Read(hash)
	if err != nil {
		return r, err
	}

	// Read the first byte to see if it's compressed or not.
	var b [1]byte
	n, err := r.Read(b[:])
	if err != nil {
		return nil, err
	}
	log.Check(n == 1)

	if b[0] == 1 {
		// Compressed: make a gzip reader.
		gzr := readerPool.Get().(*gzip.Reader)
		err = gzr.Reset(r)
		if err != nil {
			return nil, err
		}
		return &zipReaderAndCloser{gzr, r}, nil
	} else {
		// Otherwise just read the rest of the data normally.
		return r, nil
	}
}

type zipReaderAndCloser struct {
	gzr *gzip.Reader
	c   io.Closer
}

func (z *zipReaderAndCloser) Read(b []byte) (int, error) {
	return z.gzr.Read(b)
}

func (z *zipReaderAndCloser) Close() error {
	readerPool.Put(z.gzr)
	return z.c.Close()
}

func (c *compressed) WriteMetadata(name string, data []byte) {
	// Metadata contents aren't compressed, so just pass this along
	// directly.
	c.backend.WriteMetadata(name, data)
}

func (c *compressed) ReadMetadata(name string) []byte {
	return c.backend.ReadMetadata(name)
}

func (c *compressed) MetadataExists(name string) bool {
	return c.backend.MetadataExists(name)
}

func (c *compressed) ListMetadata() map[string]time.Time {
	return c.backend.ListMetadata()
}
