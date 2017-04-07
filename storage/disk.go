// storage/disk.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package storage

import (
	"bytes"
	"github.com/mmp/bk/rdso"
	u "github.com/mmp/bk/util"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// The Reed-Solomon encoding implementation ends up reading the whole file
// into memory (and more), so limit the size of packfiles to 2GB for now,
// which makes sure things aren't too bad.
const MaxDiskPackFileSize = 1 << 31

type disk struct {
	backupDir  string
	chunkIndex ChunkIndex

	indexFile    *robustWriter
	packFile     *robustWriter
	blobPacker   BlobPacker
	packFileSize int64
	packFileName string

	bytesSaved int64
	blobsSaved int
}

// NewDisk returns a new storage.Backend that stores data to the given
// backupDir. This directory should be empty the first time NewDisk is
// called with it.
func NewDisk(backupDir string) Backend {
	// Make sure that the backup directory exists and is in fact a directory.
	stat, err := os.Stat(backupDir)
	log.CheckError(err)
	if stat.IsDir() == false {
		log.Fatal(backupDir + ": is a regular file")
	}

	entries, err := ioutil.ReadDir(backupDir)
	if len(entries) == 0 {
		// Create the directories we'll need in the following
		for _, d := range []string{"packs", "indices", "metadata"} {
			path := filepath.Join(backupDir, d)
			log.CheckError(os.Mkdir(path, 0700))
		}
	} else {
		// It should be just those three directories...
		log.Check(len(entries) == 3)
	}

	db := &disk{
		backupDir: backupDir,
	}

	// Use the indices to figure out which chunks we've already got on
	// disk.
	indicesDir := path.Join(db.backupDir, "indices")
	fileinfo, err := ioutil.ReadDir(indicesDir)
	log.CheckError(err)
	for _, file := range fileinfo {
		path := filepath.Join(indicesDir, file.Name())
		if !strings.HasSuffix(path, ".idx") {
			if !strings.HasSuffix(path, ".rs") {
				log.Warning("%s: non-idx/rs file in indices directory", path)
			}
			continue
		}
		log.Check(!file.IsDir())

		idxContents, err := ioutil.ReadFile(path)
		log.CheckError(err)

		baseName := strings.TrimSuffix(file.Name(), ".idx")
		db.chunkIndex.AddIndexFile(db.packPath(baseName), idxContents)
	}

	return db
}

func (db *disk) packPath(base string) string {
	return filepath.Join(db.backupDir, "packs", base+".pack")
}

func (db *disk) idxPath(base string) string {
	return filepath.Join(db.backupDir, "indices", base+".idx")
}

func (db *disk) String() string {
	return "disk: " + db.backupDir
}

func (db *disk) LogStats() {
	if db.blobsSaved > 0 {
		log.Print("saved %s bytes in %d blobs (avg %.1f B / blob)",
			u.FmtBytes(db.bytesSaved), db.blobsSaved,
			float64(db.bytesSaved)/float64(db.blobsSaved))
	}
}

func (db *disk) Write(data []byte) Hash {
	hash := HashBytes(data)

	// If this data has already been stored, then we can just return its
	// hash immediately and not write it again.
	if _, err := db.chunkIndex.Lookup(hash); err == nil {
		return hash
	}

	// Stop writing to the current pack file if this blob is going to make
	// it be too big. 32 is some extra slop for the magic number, etc.
	if db.packFileSize+int64(len(data))+32 >= MaxDiskPackFileSize {
		db.closeFiles()
	}

	// Open up a new pack and index file if needed.
	if db.packFile == nil {
		// This gives us a unique filename--this hash isn't in storage and
		// thus couldn't have earlier been used for a filename.
		hashStr := hash.String()
		db.blobPacker = BlobPacker{}

		db.packFileName = db.packPath(hashStr)
		db.packFileSize = 0
		db.packFile = newRobustWriter(db.packFileName)
		db.indexFile = newRobustWriter(db.idxPath(hashStr))
	}

	idx, pack := db.blobPacker.Pack(hash, data)
	db.indexFile.Write(idx)
	db.packFile.Write(pack)

	// Subtract 1 since the current pack file is the last one in
	// allPackfiles...
	db.chunkIndex.AddSingle(hash, db.packFileName, db.packFileSize, int64(len(pack)))

	db.packFileSize += int64(len(pack))

	// Update stats
	db.bytesSaved += int64(len(data))
	db.blobsSaved++

	return hash
}

func (db *disk) closeFiles() {
	if db.packFile != nil {
		// Important: close the pack file first to make sure it is
		// successfully and safely on disk before finalizing the index
		// file.
		db.packFile.Close()
		db.indexFile.Close()
		db.packFile = nil
		db.indexFile = nil
	}
}

func (db *disk) SyncWrites() {
	db.closeFiles()
}

func (db *disk) HashExists(hash Hash) bool {
	_, err := db.chunkIndex.Lookup(hash)
	return err == nil
}

func (db *disk) Hashes() map[Hash]struct{} {
	return db.chunkIndex.Hashes()
}

func (db *disk) Read(hash Hash) (io.ReadCloser, error) {
	b, err := db.readChunk(hash)
	if err != nil {
		return nil, err
	}
	return ioutil.NopCloser(bytes.NewReader(b)), nil
}

func (db *disk) readChunk(hash Hash) ([]byte, error) {
	loc, err := db.chunkIndex.Lookup(hash)
	if err != nil {
		return nil, err
	}

	// Open the pack file for the blob and seek to the offset where it
	// starts.
	f, err := os.Open(loc.PackName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	_, err = f.Seek(loc.Offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// Grab its data.
	blob := make([]byte, loc.Length)
	_, err = io.ReadFull(f, blob)
	if err != nil {
		return nil, err
	}

	chunk, err := DecodeBlob(blob)
	if err != nil {
		return nil, err
	}

	// Make sure the stored hash matches the hash we're looking for.
	if HashBytes(chunk) != hash {
		return nil, ErrHashMismatch
	}

	return chunk, nil
}

func (db *disk) Fsck() {
	// Check the Reed-Solomon encoding of all of the (non-.rs) files.
	log.Verbose("Checking Reed-Solomon codes of all files")
	filepath.Walk(db.backupDir,
		func(path string, info os.FileInfo, err error) error {
			if strings.HasSuffix(path, ".rs") {
				return nil
			} else {
				return rdso.CheckFile(path, path+".rs", log)
			}
		})

	// Make sure each blob is available in a pack file and that its data's
	// hash matches the stored hash.
	allHashes := db.chunkIndex.Hashes()
	log.Verbose("Checking %d blobs in pack files", len(allHashes))
	for hash := range allHashes {
		fsckHash(hash, db)
	}

	// Go through all of the pack files and make sure all chunks are present
	// in an index.
	packDir := filepath.Join(db.backupDir, "packs")
	entries, err := ioutil.ReadDir(packDir)
	if err != nil {
		log.Error("%s: %s", packDir, err.Error())
		return
	}

	log.Verbose("Checking index completeness for chunks in %d pack files.",
		len(entries)/2)
	for _, info := range entries {
		if strings.HasSuffix(info.Name(), ".rs") {
			continue
		}
		if !strings.HasSuffix(info.Name(), ".pack") {
			log.Error("%s: file in packs/ not .rs and not .pack file", info.Name())
			continue
		}

		path := filepath.Join(packDir, info.Name())
		f, err := os.Open(path)
		if err != nil {
			log.Error("%s: %s", path, err)
			continue
		}

		fsckPackFile(f, allHashes)

		f.Close()
	}
}

func (db *disk) WriteMetadata(name string, data []byte) {
	if db.MetadataExists(name) {
		log.Fatal("%s: metadata already exists", name)
	}
	path := filepath.Join(db.backupDir, "metadata", name)
	w := newRobustWriter(path)
	_, err := w.Write(data)
	log.CheckError(err)
	w.Close()
}

func (db *disk) ReadMetadata(name string) []byte {
	path := filepath.Join(db.backupDir, "metadata", name)
	b, err := ioutil.ReadFile(path)
	log.CheckError(err)
	return b
}

func (db *disk) MetadataExists(name string) bool {
	path := filepath.Join(db.backupDir, "metadata", name)
	_, err := os.Stat(path)
	return err == nil
}

func (db *disk) ListMetadata() map[string]time.Time {
	m := make(map[string]time.Time)

	mdDir := path.Join(db.backupDir, "metadata")
	fileinfo, err := ioutil.ReadDir(mdDir)
	log.CheckError(err)

	for _, info := range fileinfo {
		log.Check(!info.IsDir())
		// Skip Reed-Solomon encoded files.
		if !strings.HasSuffix(info.Name(), ".rs") {
			m[info.Name()] = info.ModTime()
		}
	}

	return m
}
