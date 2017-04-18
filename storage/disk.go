// storage/disk.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package storage

import (
	"github.com/mmp/bk/rdso"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// The Reed-Solomon encoding implementation ends up reading the whole file
// into memory (and more), so limit the size of packfiles to 2GB for now,
// which makes sure things aren't too bad.
const maxDiskPackFileSize = 1 << 31

// disk implements the FileStorage interface to store data in a directory
// in the local file system.
type disk struct {
	dir string
}

// NewDisk returns a new storage.Backend that stores data to the given
// dir. This directory should be empty the first time NewDisk is
// called with it.
func NewDisk(dir string) Backend {
	// Make sure that the backup directory exists and is in fact a directory.
	stat, err := os.Stat(dir)
	log.CheckError(err)
	if stat.IsDir() == false {
		log.Fatal("%s: is a regular file", dir)
	}

	entries, err := ioutil.ReadDir(dir)
	if len(entries) == 0 {
		// Create the directories we'll need in the following
		for _, d := range []string{"packs", "indices", "metadata"} {
			path := filepath.Join(dir, d)
			log.CheckError(os.Mkdir(path, 0700))
		}
	} else {
		// It should be just those three directories...
		log.Check(len(entries) == 3,
			"%s: unexpected contents found in backup directory", dir)
	}

	return newPackFileBackend(&disk{dir}, maxDiskPackFileSize)
}

func (db *disk) ForFiles(prefix string, f func(n string, created time.Time)) {
	// Assume that the prefix specifies a directory; read its contents.
	dir := filepath.Join(db.dir, prefix)
	fileinfo, err := ioutil.ReadDir(dir)
	log.CheckError(err)

	for _, file := range fileinfo {
		if strings.HasSuffix(file.Name(), ".rs") {
			// Don't pass the Reed-Solomon files back.
			continue
		}
		log.Check(!file.IsDir())

		f(filepath.Join(prefix, file.Name()), file.ModTime())
	}
}

func (db *disk) String() string {
	return "disk: " + db.dir
}

func (db *disk) Fsck() bool {
	// Check the Reed-Solomon encoding of all of the (non-.rs) files.
	log.Verbose("Checking Reed-Solomon codes of all files")
	filepath.Walk(db.dir,
		func(path string, info os.FileInfo, err error) error {
			if !strings.HasSuffix(path, ".rs") {
				rdso.CheckFile(path, path+".rs", log)
			}
			return nil
		})
	return true
}

func (db *disk) CreateFile(name string) RobustWriteCloser {
	return newRobustDiskWriter(filepath.Join(db.dir, name))
}

func (db *disk) ReadFile(name string, offset int64, length int64) ([]byte, error) {
	f, err := os.Open(filepath.Join(db.dir, name))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if length != 0 {
		// Return just a segment of the file contents.
		_, err = f.Seek(offset, io.SeekStart)
		if err != nil {
			return nil, err
		}
		contents := make([]byte, length)
		_, err = io.ReadFull(f, contents)
		return contents, err
	}

	return ioutil.ReadAll(f)
}

// robustWriter implements the io.WriteCloser interface to write a file on
// disk. Its implementations of Write and Close never return errors; any
// errors encountered are treated as fatal errors. It ensures that a
// incomplete version of the file is never left behind, even in the event
// of a fatal error or other interruption. After a call to Close returns,
// the caller can be certain that all of the bytes written have
// successfully landed on disk.
type robustWriter struct {
	file *os.File
	path string
}

func newRobustDiskWriter(path string) RobustWriteCloser {
	errorIfExists(path)

	// Open a temporary file to hold the intermediate writes.
	tmpPath := path + ".tmp"
	errorIfExists(tmpPath)
	f, err := os.Create(tmpPath)
	log.CheckError(err)

	return &robustWriter{f, path}
}

func (w *robustWriter) Write(b []byte) {
	_, err := w.file.Write(b)
	log.CheckError(err)
}

func (w *robustWriter) Close() {
	// When it's time to close the writer, first make sure that all of the
	// writes have landed on disk in the temporary file.
	log.CheckError(w.file.Sync())
	log.CheckError(w.file.Close())

	// Next, compute the Reed-Solomon encoding for the file's contents.
	const nDataShards = 17
	const nParityShards = 3
	const hashRate = 1024 * 1024
	tmpPath := w.path + ".tmp"
	rdso.EncodeFile(tmpPath, w.path+".rs", nDataShards, nParityShards,
		hashRate)

	// Finally, rename the temporary file (which we now know to be valid
	// and complete) to the final filename that we wanted originally. Only
	// once the rename has succeeded can we be sure that everything is
	// safely on disk.
	log.CheckError(os.Rename(tmpPath, w.path))
}
