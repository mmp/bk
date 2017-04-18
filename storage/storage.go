// storage/storage.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package storage

import (
	"encoding/hex"
	"errors"
	u "github.com/mmp/bk/util"
	"golang.org/x/crypto/sha3"
	"io"
	"io/ioutil"
	"os"
	"time"
)

var (
	ErrHashNotFound       = errors.New("hash not found")
	ErrHashMismatch       = errors.New("hash value mismatch")
	ErrIndexMagicWrong    = errors.New("index entry has incorrect magic number")
	ErrBlobMagicWrong     = errors.New("blob has incorrect magic number")
	ErrPrematureEndOfData = errors.New("premature end of data")
)

///////////////////////////////////////////////////////////////////////////
// Logging

var log *u.Logger

func SetLogger(l *u.Logger) {
	log = l
}

///////////////////////////////////////////////////////////////////////////
// Hashing

// HashSize is the number of bytes in the hash values returned to
// represent chunks of data.
const HashSize = 32

// Hash encodes a fixed-size secure hash of a collection of bytes.
type Hash [HashSize]byte

func NewHash(b []byte) (h Hash) {
	log.Check(len(b) == len(h))
	copy(h[:], b)
	return h
}

// HashBytes computes the SHAKE256 hash of the given byte slice.
func HashBytes(b []byte) Hash {
	var h Hash
	sha3.ShakeSum256(h[:], b)
	return h
}

// String returns the given Hash as a hexidecimal-encoded string.
func (h Hash) String() string {
	return hex.EncodeToString(h[:])
}

///////////////////////////////////////////////////////////////////////////
// Interface to storage backends

// Backend describes a general interface for low-level data storage;
// users can provide chunks of data that a storage backend will store
// (on disk, in the cloud, etc.), and are returned a Hash that identifies
// each such chunk. Implementations should apply deduplication so that if
// the same chunk is supplied multiple times, it will only be stored once.
//
// Note: it isn't safe in general for multiple threads to call Backend
// methods concurrently, though the Read() method may be called by multiple
// threads (as long as others aren't calling other Backend methods).
type Backend interface {
	// String returns the name of the Backend in the form of a string.
	String() string

	// LogStats reports any statistics that the Backend may have gathered
	// during the course of its operation.
	LogStats()

	// Fsck checks the consistency of the data in the Backend and reports
	// any problems found via the logger specified by SetLogger.
	Fsck()

	// Write saves the provided chunk of data to storage, returning a Hash
	// that uniquely identifies it. Any write errors are fatal and
	// terminate the program.
	Write(chunk []byte) Hash

	// SyncWrites ensures that all chunks of data provided to Write have
	// in fact reached permanent storage. Calls to Read may not find
	// data stored by Write if SyncWrites hasn't been called after the
	// call to Write.
	SyncWrites()

	// Read returns a io.ReadCloser that provides the chunk for the given
	// hash. If the given hash doesn't exist in the backend, an error is
	// returned.
	Read(hash Hash) (io.ReadCloser, error)

	// HashExists reports whether a blob of data with the given hash exists
	// in the storage backend.
	HashExists(hash Hash) bool

	// Hashes returns a map that has all of the hashes stored by the
	// storage backend.
	Hashes() map[Hash]struct{}

	// WriteMetadata saves the given data in the storage backend,
	// associating it with the given name. It's mostly used for storing
	// data that we don't want to run through the dedupe process and want
	// to be able to easily access directly by name.
	WriteMetadata(name string, data []byte)

	// ReadMetadata returns the metadata for a given name that was stored
	// with WriteMetadata.
	ReadMetadata(name string) []byte

	// MetadataExists indicates whether the given named metadata is
	// present in the storage backend.
	MetadataExists(name string) bool

	// ListMetadata returns a map from all of the existing metadata
	// to the time each one was created.
	ListMetadata() map[string]time.Time
}

///////////////////////////////////////////////////////////////////////////
// Some utility stuff

func errorIfExists(path string) {
	_, err := os.Stat(path)
	if err == nil {
		log.Fatal(path + ": file exists")
	}
}

type readerAndCloser struct {
	io.Reader
	io.Closer
}

///////////////////////////////////////////////////////////////////////////

// NewHashesReader returns an io.ReadCloser that reads multiple hashes in
// parallel from the given storage backend. It supplies the bytes of the
// hashes' chunks concatenated together into a single stream.  If non-nil,
// the sem parameter is used to limit the number of active readers;
// otherwise a fixed number of reader goroutines are launched.
func NewHashesReader(hashes []Hash, sem chan bool, backend Backend) io.ReadCloser {
	// If it's just one hash, don't do anything fancy.
	if len(hashes) == 1 {
		r, err := backend.Read(hashes[0])
		log.CheckError(err, "%s: %s", hashes[0], err)
		return r
	}

	// Limit the maximum number of concurrent readers.
	nReaders := 32
	if len(hashes) < nReaders {
		nReaders = len(hashes)
	}

	cin := make(chan hashIndex, len(hashes))
	r := &parallelReader{
		m:        make(map[int][]byte),
		maxIndex: len(hashes),
		cout:     make(chan indexData, 4)}

	// Launch readers.
	for i := 0; i < nReaders; i++ {
		if i == 0 {
			// Always pass a nil sem for the first reader, since we assume
			// that the caller is allowed to be reading.
			go preader(backend, nil, cin, r.cout)
		} else {
			go preader(backend, sem, cin, r.cout)
		}
	}

	for i, h := range hashes {
		// Hash indices are assigned in order so that we can construct a
		// bytestream that has the correct order.
		cin <- hashIndex{hash: h, index: i}
	}
	close(cin)

	return r
}

type parallelReader struct {
	// Hash indices to []bytes. The map stores bytes for hashes that we've
	// gotten from the readers, including ones that we're not ready
	// to return yet since we don't have the predecessors yet.
	m map[int][]byte
	// Hash index to return the bytes for before going to the next one.
	index    int
	maxIndex int
	// Result channel that the goroutines threads send results along.
	cout chan indexData
}

type hashIndex struct {
	hash  Hash
	index int
}

type indexData struct {
	index int
	data  []byte
}

func preader(backend Backend, sem chan bool, cin chan hashIndex,
	cout chan indexData) {
	for {
		if sem != nil {
			// Block until we're allowed to read
			sem <- true
		}

		// Get the next hash to read.
		hi, ok := <-cin
		if !ok {
			// No more.
			if sem != nil {
				// Let someone else read.
				<-sem
			}
			return
		}

		r, err := backend.Read(hi.hash)
		log.CheckError(err)
		data, err := ioutil.ReadAll(r)
		log.CheckError(err)
		log.CheckError(r.Close())

		if sem != nil {
			// Let someone else read.
			<-sem
		}

		// Send the result out on the result chan.
		cout <- indexData{hi.index, data}
	}
}

func (r *parallelReader) Read(buf []byte) (int, error) {
	if r.index == r.maxIndex {
		// We've read everything.
		return 0, io.EOF
	}

	// Try to get the []byte for the current hash index.
	if chunk, ok := r.m[r.index]; !ok {
		// Don't have it. Read from the result chan (and block if there's
		// nothing ready yet).
		id := <-r.cout
		// What we got may or may not be the one we're waiting for; record
		// it in the map and go 'round again.
		r.m[id.index] = id.data
		// Try again
		return r.Read(buf)
	} else {
		// We have bytes for the current index; return some to the caller.
		n := copy(buf, chunk)
		if n < len(chunk) {
			// More left for the next Read() call.
			r.m[r.index] = chunk[n:]
		} else {
			// Done with this index; move to the next.
			delete(r.m, r.index)
			r.index++
		}
		return n, nil
	}
}

func (r *parallelReader) Close() error {
	// It's grungy to not have a cleaner way to shutdown the readers in the
	// middle and to drain it all out this way, but we don't really need that
	// functionality at the moment anyway.
	_, err := ioutil.ReadAll(r)
	return err
}

///////////////////////////////////////////////////////////////////////////
// Consistency checking

func fsckHash(hash Hash, backend Backend) {
	rc, err := backend.Read(hash)
	if err != nil {
		log.Error("%s: %s", hash, err)
		return
	}

	chunk, err := ioutil.ReadAll(rc)
	if err != nil {
		rc.Close()
		log.Error("%s: %s", hash, err)
		return
	}

	if HashBytes(chunk) != hash {
		log.Error("%s: hash mismatch", hash)
	}

	if err = rc.Close(); err != nil {
		log.Error("%s: %s", hash, err)
	}
}
