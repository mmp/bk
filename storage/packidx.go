// storage/packidx.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	u "github.com/mmp/bk/util"
	"io"
	"io/ioutil"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var IdxMagic = [4]byte{'I', 'd', 'x', '2'}
var BlobMagic = [4]byte{'B', 'L', '0', 'B'}

/*
File format specs:
- Pack file: for each chunk, stores BlobMagic, the length of the chunk encoded
  as a varint, and then the chunk contents.
- Index file: for each chunk, stores IdxMagic, the hash, and then the offset
  into the pack file and the length of the chunk, both encoded as varints.

Note: index files can be recreated from pack files alone (but no code
exists to do this currently.)
*/

// PackBLob takes (hash, chunk) pairs and the current size of the pack file
// and converts them to the representation to be stored in index and pack
// files, returning the bytes to append to the index and back files to
// store the chunk.
func PackBlob(h Hash, chunk []byte, packFileSize int64) (idx, pack []byte) {
	idxAlloc := len(IdxMagic) + HashSize + 2*binary.MaxVarintLen64
	packAlloc := len(BlobMagic) + binary.MaxVarintLen64 + len(chunk)
	idx = make([]byte, idxAlloc)
	pack = make([]byte, packAlloc)

	// Pack file: magic number, data length, chunk
	np := copy(pack, BlobMagic[:])
	np += binary.PutVarint(pack[np:], int64(len(chunk)))
	np += copy(pack[np:], chunk)
	pack = pack[:np]

	// Index file: magic number, hash, pack offset, pack read size
	ni := copy(idx, IdxMagic[:])
	ni += copy(idx[ni:], h[:])
	ni += binary.PutVarint(idx[ni:], packFileSize)
	ni += binary.PutVarint(idx[ni:], int64(len(pack)))
	idx = idx[:ni]

	return
}

///////////////////////////////////////////////////////////////////////////

// ChunkIndex maintains an index from hashes to the locations of their blobs
// in pack files.
type ChunkIndex struct {
	hashToLoc map[Hash]blobLoc
	nameToId  map[string]int
	idToName  []string
}

// Internal representation for the location of a blob, using an integer
// rather than a string to identify pack files, for compactness.
type blobLoc struct {
	packId int
	offset int64
	length int64
}

// External representation of the location of a blob in a pack file that's
// returned to callers.
type BlobLocation struct {
	PackName string
	Offset   int64
	Length   int64
}

func (c *ChunkIndex) AddSingle(hash Hash, packName string, offset, length int64) {
	if c.hashToLoc == nil {
		c.hashToLoc = make(map[Hash]blobLoc)
		c.nameToId = make(map[string]int)
	}

	if _, ok := c.hashToLoc[hash]; ok {
		log.Fatal("%s: hash already in ChunkIndex", hash)
	}

	id, ok := c.nameToId[packName]
	if !ok {
		// First time we've seen this packName
		id = len(c.idToName)
		c.nameToId[packName] = id
		c.idToName = append(c.idToName, packName)
	}

	c.hashToLoc[hash] = blobLoc{id, offset, length}
}

// Takes the entire contents of an index file and associates its index
// entries with the given pack file name. Returns the number of entries
// added and the error (if any).
func (c *ChunkIndex) AddIndexFile(packName string, idx []byte) (int, error) {
	added := 0
	for len(idx) > 0 {
		if !bytes.Equal(idx[:len(IdxMagic)], IdxMagic[:]) {
			return added, ErrIndexMagicWrong
		}
		idx = idx[len(IdxMagic):]

		var hash Hash
		n := copy(hash[:], idx)
		if n < HashSize {
			return added, ErrPrematureEndOfData
		}

		offset, nvar := binary.Varint(idx[n:])
		if nvar <= 0 {
			return added, fmt.Errorf("varint: returned %d", nvar)
		}
		n += nvar

		length, nvar := binary.Varint(idx[n:])
		if nvar <= 0 {
			return added, fmt.Errorf("varint: returned %d", nvar)
		}
		n += nvar

		c.AddSingle(hash, packName, offset, length)
		added++

		idx = idx[n:]
	}
	return added, nil
}

func (c *ChunkIndex) Lookup(hash Hash) (BlobLocation, error) {
	loc, ok := c.hashToLoc[hash]
	if !ok {
		return BlobLocation{}, ErrHashNotFound
	}

	return BlobLocation{c.idToName[loc.packId], loc.offset, loc.length}, nil
}

func (c *ChunkIndex) Hashes() map[Hash]struct{} {
	m := make(map[Hash]struct{})
	for h := range c.hashToLoc {
		var empty struct{}
		m[h] = empty
	}
	return m
}

// DecodeBlob takes a blob read from a pack file (as per the specs from a
// BlobLocation) and returns the chunk stored in that blob.
func DecodeBlob(blob []byte) ([]byte, error) {
	return decodeOneBlob(bytes.NewReader(blob))
}

type byteAndRegularReader interface {
	Read([]byte) (int, error)
	ReadByte() (byte, error)
}

// Given a reader for a pack file, decodes it into blobs and then calls the
// given callback function for each blob's chunk.
func DecodePackFile(r io.Reader, f func(chunk []byte)) error {
	br, ok := r.(byteAndRegularReader)
	if !ok {
		br = bufio.NewReader(r)
	}

	for {
		chunk, err := decodeOneBlob(br)
		switch err {
		case nil:
			f(chunk)
		case io.EOF:
			return nil
		default:
			return err
		}
	}
}

// Returns the chunk and nil on success, a nil chunk and io.EOF on a "clean"
// EOF, and a non-nil error otherwise.
func decodeOneBlob(r byteAndRegularReader) ([]byte, error) {
	var magic [4]byte
	_, err := io.ReadFull(r, magic[:])
	if err != nil {
		return nil, err
	}
	if magic != BlobMagic {
		return nil, ErrBlobMagicWrong
	}

	length, err := binary.ReadVarint(r)
	if err != nil {
		if err == io.EOF {
			return nil, ErrPrematureEndOfData
		}
		return nil, err
	}

	chunk := make([]byte, length)
	_, err = io.ReadFull(r, chunk)
	if err != nil {
		if err == io.EOF {
			return nil, ErrPrematureEndOfData
		}
		return nil, err
	}

	return chunk, nil
}

///////////////////////////////////////////////////////////////////////////

// PackFileBackend implements the storage.Backend interface, but depends on
// an implementation of the FileStorage interface to handle the mechanics
// of storing and retrieving files.  In turn, we can implement
// functionality that's common between the disk and GCS backends in a
// single place.
type PackFileBackend struct {
	fs    FileStorage
	start time.Time

	metadataNames map[string]time.Time
	chunkIndex    ChunkIndex

	// Names of the current pack and index files.
	packName, idxName string
	// Contents of the index file are buffered and not written until
	// after the corresponding pack file has safely landed in storage;
	// this ensures that we don't inadvertently save an index for a
	// pack file that never makes it to storage.
	idx []byte
	// Contents of the current pack file are immediately sent along packChan,
	// where the writer should be landing them in storage.
	packChan chan []byte
	// Size of the pack file currently being written to.
	packSize    int64
	maxPackSize int64

	// A goroutine is launched to perform asynchronous writes. It reads
	// file write requests from writeChan and lands them in storage in the
	// order received.
	writeChan chan fileWrite
	wg        sync.WaitGroup

	// mu protects the statistics variables.
	mu                    sync.Mutex
	bytesSaved, bytesRead int64
	numSaves, numReads    int
}

// Represents a file to be written to in the storage system. The file's
// contents should be provided as one or more slices sent along the chan
// provided.
type fileWrite struct {
	path string
	ch   chan []byte
}

// RobustWriteCloser is like a io.WriteCloser, except it treats any errors
// as fatal errors and thus doesn't have error return values. Write()
// always writes all bytes given to it, and after a call to Close()
// returns, the contents have successfully been committed to storage.
type RobustWriteCloser interface {
	Write(b []byte)
	Close()
}

// FileStorage is a simple abstraction for a storage system.
type FileStorage interface {
	// CreateFile returns a RobustWriteCloser for a file with the given name;
	// a fatal error occurs if a file with that name already exists.
	CreateFile(name string) RobustWriteCloser

	// ReadFile returns the contents of the given file. If length is zero, the
	// whole file contents are returned; otherwise the segment starting at offset
	// with given length is returned.
	//
	// TODO: it might be more idiomatic to return e.g. an io.ReadCloser,
	// but between the GCS backend needing to be able to retry reads and
	// the fact that callers usually want a []byte in the end anyway, this
	// seems more straightforward overall.
	ReadFile(name string, offset int64, length int64) ([]byte, error)

	// ForFiles calls the given callback function for all files with the
	// given directory prefix, providing the file path and its creation
	// time.
	ForFiles(prefix string, f func(path string, created time.Time))

	String() string

	// Fsck checks the validity of the stored data.  The returned Boolean
	// value indicates whether or not the caller should continue and
	// perform its own checks on the contents of the data as well.
	Fsck() bool
}

func newPackFileBackend(fs FileStorage, maxPackSize int64) Backend {
	pb := &PackFileBackend{
		fs:          fs,
		start:       time.Now(),
		maxPackSize: maxPackSize,
	}

	// Get all of the the names of the metadata.
	pb.metadataNames = make(map[string]time.Time)
	pb.fs.ForFiles("metadata/", func(n string, created time.Time) {
		pb.metadataNames[filepath.Base(n)] = created
	})

	// TODO: do in parallel?
	log.Verbose("Starting to read indices.")
	added := 0
	pb.fs.ForFiles("indices/", func(n string, created time.Time) {
		if !strings.HasSuffix(n, ".idx") {
			log.Warning("%s: non .idx file found in indices/ directory", n)
			return
		}

		idx, err := pb.fs.ReadFile(n, 0, 0)
		if err != nil {
			log.Error("%s: %+v", n, err)
		}
		log.CheckError(err)

		log.Debug("%s: got %d-length index file.", n, len(idx))
		base := filepath.Base(strings.TrimSuffix(n, ".idx"))
		nadd, err := pb.chunkIndex.AddIndexFile("packs/"+base+".pack", idx)
		if err != nil {
			log.Fatal("%s: %s", n, err)
		}
		added += nadd

		pb.numReads++
		pb.bytesRead += int64(len(idx))
	})
	log.Verbose("Done reading indices: %d files, %s -> %d entries", pb.numReads,
		u.FmtBytes(pb.bytesRead), added)

	pb.launchWriter()

	return pb
}

func (pb *PackFileBackend) String() string {
	return pb.fs.String()
}

func (pb *PackFileBackend) LogStats() {
	delta := time.Since(pb.start)
	if pb.numSaves > 0 {
		upBytesPerSec := float64(pb.bytesSaved) / delta.Seconds()
		log.Print("stored %s of chunks in %d writes (avg %s, %s/s)",
			u.FmtBytes(pb.bytesSaved), pb.numSaves,
			u.FmtBytes(pb.bytesSaved/int64(pb.numSaves)),
			u.FmtBytes(int64(upBytesPerSec)))
	}
	if pb.numReads > 0 {
		downBytesPerSec := float64(pb.bytesRead) / delta.Seconds()
		log.Print("read %s in %d reads (avg %s, %s/s)",
			u.FmtBytes(pb.bytesRead), pb.numReads,
			u.FmtBytes(pb.bytesRead/int64(pb.numReads)),
			u.FmtBytes(int64(downBytesPerSec)))
	}
}

func (pb *PackFileBackend) Write(chunk []byte) Hash {
	hash := HashBytes(chunk)
	if _, err := pb.chunkIndex.Lookup(hash); err == nil {
		log.Debug("%s: hash already stored", hash)
		return hash
	}

	// 16 bytes of slop in the second test to account for magic numbers and
	// the encoded chunk length.
	if pb.packName == "" || pb.packSize+int64(len(chunk))+16 > pb.maxPackSize {
		// Close out the current pack file (if there is one).
		pb.closePack()

		// Start new pack and idx files. Using the hash for the filenames
		// gives us a guaranteed new name: since this hash isn't in
		// storage, ergo no index/pack files can have it as a name.
		pb.packName = "packs/" + hash.String() + ".pack"
		pb.packChan = make(chan []byte, 1024)
		pb.writeChan <- fileWrite{pb.packName, pb.packChan}

		pb.idxName = "indices/" + hash.String() + ".idx"
		pb.packSize = 0
	}

	idx, pack := PackBlob(hash, chunk, pb.packSize)

	// Add to the index before incrementing pb.packSize!
	pb.chunkIndex.AddSingle(hash, pb.packName, pb.packSize, int64(len(pack)))
	pb.packSize += int64(len(pack))

	// Save the index file addition in pb.idx for now, but send the pack
	// file data on to the writer immediately.
	pb.idx = append(pb.idx, idx...)
	pb.packChan <- pack

	pb.mu.Lock()
	pb.numSaves++
	pb.bytesSaved += int64(len(idx) + len(pack))
	pb.mu.Unlock()

	return hash
}

func (pb *PackFileBackend) closePack() {
	if pb.packChan != nil {
		close(pb.packChan)
		pb.packChan = nil

		// Now send along the index file to be written, only after the pack
		// file has been successfully saved to storage.
		log.Check(len(pb.idx) > 0)
		idxChan := make(chan []byte, 1)
		pb.writeChan <- fileWrite{pb.idxName, idxChan}
		idxChan <- pb.idx
		close(idxChan)

		pb.packName = ""
		pb.idxName = ""
		pb.packSize = 0
		pb.idx = nil
	}
}

func (pb *PackFileBackend) launchWriter() {
	log.Check(pb.writeChan == nil)
	// Don't allow too much buffering here: each 2 of these may hold an
	// entire pack and index file's contents buffered in their internal
	// chans.
	pb.writeChan = make(chan fileWrite, 4)

	pb.wg.Add(1)
	go writeWorker(pb.fs, pb.writeChan, &pb.wg)
}

func writeWorker(fs FileStorage, ch chan fileWrite, wg *sync.WaitGroup) {
	for {
		item, ok := <-ch
		if !ok {
			// No more writes to come.
			wg.Done()
			return
		}

		// Got a new file to start writing to.
		w := fs.CreateFile(item.path)
		for {
			// Grab byte slices from the chan for that file and write them
			// until that chan is closed.
			if b, ok := <-item.ch; ok {
				w.Write(b)
			} else {
				w.Close()
				// On to the next file.
				break
			}
		}
	}
}

func (pb *PackFileBackend) SyncWrites() {
	// Wrap up the current pack file and save its index file.
	pb.closePack()

	// Close the chan and wait for the writer to exit, at which point all
	// pending writes have landed in storage.
	close(pb.writeChan)
	pb.wg.Wait()
	pb.writeChan = nil

	pb.launchWriter()
}

func (pb *PackFileBackend) Read(hash Hash) (io.ReadCloser, error) {
	if loc, err := pb.chunkIndex.Lookup(hash); err != nil {
		return nil, err
	} else {
		blob, err := pb.fs.ReadFile(loc.PackName, loc.Offset, loc.Length)
		if err != nil {
			return nil, err
		}

		pb.mu.Lock()
		pb.numReads++
		pb.bytesRead += loc.Length
		pb.mu.Unlock()

		chunk, err := DecodeBlob(blob)
		if err != nil {
			return nil, err
		}
		if HashBytes(chunk) != hash {
			return nil, ErrHashMismatch
		}

		return ioutil.NopCloser(bytes.NewReader(chunk)), nil
	}
}

func (pb *PackFileBackend) HashExists(hash Hash) bool {
	_, err := pb.chunkIndex.Lookup(hash)
	return err == nil
}

func (pb *PackFileBackend) Hashes() map[Hash]struct{} {
	return pb.chunkIndex.Hashes()
}

func (pb *PackFileBackend) Fsck() {
	if !pb.fs.Fsck() {
		return
	}

	// Make sure each blob is available in a pack file and that its data's
	// hash matches the stored hash.
	allHashes := pb.chunkIndex.Hashes()
	log.Verbose("Checking the availability and integrity of %d blobs.",
		len(allHashes))
	for hash := range allHashes {
		// Read() makes sure the hash matches the hash of the contents.
		rc, err := pb.Read(hash)
		if err != nil {
			log.Error("%s: %s", hash, err)
			continue
		}

		_, err = ioutil.ReadAll(rc)
		if err != nil {
			rc.Close()
			log.Error("%s: %s", hash, err)
			continue
		}

		if err = rc.Close(); err != nil {
			log.Error("%s: %s", hash, err)
		}
	}

	// Go through all of the pack files and make sure all blobs are present
	// in an index.
	pb.fs.ForFiles("packs/", func(n string, created time.Time) {
		if !strings.HasSuffix(n, ".pack") {
			log.Warning("%s: non .pack file found in packs/ directory", n)
			return
		}

		// It's slightly annoying to read the whole pack file into memory
		// here, but they're not too huge. If this was a problem, we could
		// implement an io.Reader that grabbed pieces of it in turn using
		// the (start, length) arguments to ReadFile().
		pack, err := pb.fs.ReadFile(n, 0, 0)
		log.CheckError(err)
		err = DecodePackFile(bytes.NewReader(pack), func(chunk []byte) {
			hash := HashBytes(chunk)
			if _, ok := allHashes[hash]; !ok {
				log.Error("%s: hash found in pack file, but not in index", hash)
			}
		})
		if err != nil {
			log.Error("%s", err)
		}
	})
}

func (pb *PackFileBackend) WriteMetadata(name string, contents []byte) {
	if _, ok := pb.metadataNames[name]; ok {
		log.Fatal("%s: metadata already exists", name)
	}

	// The next time we run the reported creation time will be slightly
	// different, since time.Now() isn't necessarily the same time it lands
	// on disk. Presumably that's fine.
	pb.metadataNames[name] = time.Now()

	w := pb.fs.CreateFile("metadata/" + name)
	w.Write(contents)
	w.Close()
}

func (pb *PackFileBackend) ReadMetadata(name string) []byte {
	b, err := pb.fs.ReadFile("metadata/"+name, 0, 0)
	log.CheckError(err)
	return b
}

func (pb *PackFileBackend) ListMetadata() map[string]time.Time {
	return pb.metadataNames
}

func (pb *PackFileBackend) MetadataExists(name string) bool {
	_, ok := pb.metadataNames[name]
	return ok
}
