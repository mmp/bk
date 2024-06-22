// cmd/bk/backup.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/mmp/bk/storage"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

///////////////////////////////////////////////////////////////////////////
// BackupRoot

// BackupRoot is where it all starts; one of these is serialized and
// stored in the storage backend for each backup.
type BackupRoot struct {
	Dir  DirEntry
	Time time.Time
}

// NewRoot creates a new BackupRoot (as is done when doing a new backup).
func NewRoot(dirpath string) (BackupRoot, error) {
	fi, err := os.Stat(dirpath)
	if err != nil {
		return BackupRoot{}, err
	}
	if !fi.IsDir() {
		return BackupRoot{}, errors.New("not a directory")
	}

	root := BackupRoot{Time: time.Now()}
	root.Dir, err = NewDirEntry(fi)
	root.Dir.Name = "/" // TODO: unneeded?
	return root, err
}

// Reads a BackupRoot from storage, given its hash.
func ReadRoot(hash storage.Hash, backend storage.Backend) (BackupRoot, error) {
	r, err := backend.Read(hash)
	if err != nil {
		return BackupRoot{}, err
	}

	d := gob.NewDecoder(r)
	var root BackupRoot
	if err = d.Decode(&root); err != nil {
		return root, err
	}
	return root, r.Close()
}

func (br BackupRoot) Bytes() []byte {
	var buf bytes.Buffer
	e := gob.NewEncoder(&buf)
	log.CheckError(e.Encode(br))
	return buf.Bytes()
}

///////////////////////////////////////////////////////////////////////////
// DirEntry

// Each file or directory that is in the backup has an associated DirEntry.
type DirEntry struct {
	// Just the file name, not its full path.
	Name string
	// If non-nil, stores the file's contents. Empty files have nil
	// contents and an invalid hash, so it's important to check the Size
	// before trying to make use of either.  Symlink targets are always
	// stored here.
	//
	// TODO: for directories with just a few files, is it worth serializing
	// their children directly into this?
	Contents []byte
	// For a file, if Contents is nil, Hash points to its contents.  For a
	// directory, it gives the serialized []DirEntry for the files in a
	// directory.
	Hash storage.MerkleHash
	// Not used for directories or symlinks.
	Size    int64
	ModTime time.Time
	Mode    os.FileMode
}

func NewDirEntry(fi os.FileInfo) (DirEntry, error) {
	e := DirEntry{
		Name:    fi.Name(),
		Size:    fi.Size(),
		ModTime: fi.ModTime(),
		Mode:    fi.Mode(),
	}
	if !e.IsDir() && !e.IsFile() && !e.IsSymLink() {
		return DirEntry{}, errors.New("unhandled file type")
	}
	return e, nil
}

func (e *DirEntry) IsDir() bool {
	return e.Mode.IsDir()
}

func (e *DirEntry) IsFile() bool {
	return e.Mode&os.ModeType == 0
}

func isFile(mode os.FileMode) bool {
	return mode&os.ModeType == 0
}

func (e *DirEntry) IsSymLink() bool {
	return e.Mode&os.ModeSymlink != 0
}

func writeDirEntries(entries []DirEntry, backend storage.Backend, splitBits uint) storage.MerkleHash {
	var buf bytes.Buffer
	e := gob.NewEncoder(&buf)
	log.CheckError(e.Encode(entries))
	return storage.SplitAndStore(&buf, backend, splitBits)
}

func readDirEntries(hash storage.MerkleHash, backend storage.Backend) []DirEntry {
	r := hash.NewReader(nil, backend)
	d := gob.NewDecoder(r)
	var entries []DirEntry
	log.CheckError(d.Decode(&entries))
	log.CheckError(r.Close())
	return entries
}

func (e *DirEntry) GetContentsReader(sem chan bool, backend storage.Backend) (io.ReadCloser, error) {
	if !e.IsFile() {
		return nil, errors.New("not a file")
	}
	if e.Size == 0 || e.Contents != nil {
		// TODO: I believe that a nil byte slice is ok to pass to
		// NewReader, but should check.
		return ioutil.NopCloser(bytes.NewReader(e.Contents)), nil
	}
	return e.Hash.NewReader(sem, backend), nil
}

///////////////////////////////////////////////////////////////////////////

func BackupDir(dirpath string, backend storage.Backend, splitBits uint,
	excludedPaths []string) (storage.Hash, error) {
	r, err := NewRoot(dirpath)
	if err != nil {
		return storage.Hash{}, err
	}
	r.Dir.Hash, err = backupDirContents(dirpath, nil, backend, splitBits, excludedPaths)
	if err != nil {
		return storage.Hash{}, err
	}
	return backend.Write(r.Bytes()), nil
}

func BackupDirIncremental(dirpath string, baseHash storage.Hash,
	backend storage.Backend, splitBits uint, excludedPaths []string) (storage.Hash, error) {
	r, err := NewRoot(dirpath)
	if err != nil {
		return storage.Hash{}, err
	}

	// Read the entires for the root directory in the backup being used as
	// the base.
	baseRoot, err := ReadRoot(baseHash, backend)
	if err != nil {
		return storage.Hash{}, err
	}
	baseRootEntries := readDirEntries(baseRoot.Dir.Hash, backend)

	r.Dir.Hash, err = backupDirContents(dirpath, baseRootEntries, backend, splitBits,
		excludedPaths)
	if err != nil {
		return storage.Hash{}, err
	}
	return backend.Write(r.Bytes()), nil
}

// Back up the contents of the given directory (and subdirectories)
// returning a MerkleHash that identifies the serialized []DirEntry for the
// contents. If we're unable to backup various individual files or
// directories along the way, an error is logged but a nil error is
// returned from this function; we don't want to report failure if, for
// example, we don't have permissions to read a file.
func backupDirContents(dirpath string, baseEntries []DirEntry,
	backend storage.Backend, splitBits uint, excludedPaths []string) (storage.MerkleHash, error) {
	fileinfo, err := ioutil.ReadDir(dirpath)
	if err != nil {
		return storage.MerkleHash{}, err
	}

	baseEntryMap := make(map[string]int) // from Name to index in baseEntries
	for i, be := range baseEntries {
		if _, ok := baseEntryMap[be.Name]; ok {
			log.Fatal("%s: unexpected repeat in baseEntries", be.Name)
		}
		baseEntryMap[be.Name] = i
	}

	var entries []DirEntry
	for _, f := range fileinfo {
		// Try to find a corresponding file/directory in the base backup, if
		// one was provided.
		var baseEntry *DirEntry
		if i, ok := baseEntryMap[f.Name()]; ok {
			if baseEntries[i].Mode == f.Mode() {
				baseEntry = &baseEntries[i]
			}
		}

		path := filepath.Join(dirpath, f.Name())

		isExcluded := func(path string, excludedPaths []string) bool {
			for _, excl := range excludedPaths {
				if strings.Contains(path, excl) {
					return true
				}
			}
			return false
		}
		if isExcluded(path, excludedPaths) {
			log.Verbose("%s: excluding from backup", path)
			continue
		}

		log.Debug("%s: backing up", path)
		e, err := NewDirEntry(f)
		if err != nil {
			log.Error("%s: %s", path, err)
			continue
		}

		switch {
		case e.IsDir():
			var childEntries []DirEntry
			if baseEntry != nil {
				// Get the subdirectory's contents from the base backup
				// before continuing recursively.
				childEntries = readDirEntries(baseEntry.Hash, backend)
			}
			e.Hash, err = backupDirContents(path, childEntries, backend, splitBits,
				excludedPaths)
			if err != nil {
				log.Error("%s: %s", path, err)
				continue
			}
		case e.IsFile():
			if baseEntry != nil && baseEntry.Size == f.Size() &&
				baseEntry.ModTime == f.ModTime() {
				// Things look good, so just reuse the hash/contents from
				// the base file.
				e.Hash = baseEntry.Hash
				e.Contents = baseEntry.Contents
			} else {
				// The file may have changed (different mod time) or
				// definitely did if the size changed, so go ahead and
				// split and hash the contents. If the contents are in fact
				// unchanged, we only pay for some I/O here; the dedupe
				// stuff in the storage backend will recognize that we
				// already have the data stored.
				switch {
				case f.Size() < 8192:
					// Don't bother splitting small files; this gives the
					// splitter more to work with when it gets the
					// serialized array of DirEntries for this directory.
					c, err := ioutil.ReadFile(path)
					if err != nil {
						log.Error("%s: %s", path, err)
						continue
					}
					e.Contents = c
				default:
					sb := splitBits
					if isChunkReuseUnlikely(f) {
						// For large media files and files that are already
						// compressed, split into big chunks (on average
						// 256k).  For these, we don't expect any reuse
						// across changed versions of the files over
						// multiple backups, so we might as well limit the
						// number of hashes needed.
						//
						// Note that we don't want to not split at all and
						// use a single huge chunk for the file, as that
						// would end up causing the whole file to be read
						// into memory both now and at restore time, which
						// is nice to avoid.
						sb = 18
					}
					e.Hash, err = backupFileContents(path, backend, sb)
					if err != nil {
						log.Error("%s: %s", path, err)
						continue
					}
				}
			}
		case e.IsSymLink():
			target, err := os.Readlink(path)
			if err != nil {
				log.Error("%s: %s", path, err)
				continue
			}
			e.Contents = []byte(target)
		default:
			log.Fatal("%s: uncaught unhandled file type", path)
		}

		entries = append(entries, e)
	}

	return writeDirEntries(entries, backend, splitBits), nil
}

func isChunkReuseUnlikely(f os.FileInfo) bool {
	ext := strings.ToLower(filepath.Ext(f.Name()))
	if len(ext) == 0 {
		return false
	}
	for _, e := range []string{"arw", "avi", "flv", "gif", "gz", "jpeg", "jpg", "mkv",
		"mov", "mp4", "mpeg", "mpg", "nef", "png", "raw", "wmv", "zip"} {
		if ext[1:] == e {
			return true
		}
	}
	return false
}

func backupFileContents(path string, backend storage.Backend, splitBits uint) (storage.MerkleHash, error) {
	f, err := os.Open(path)
	if err != nil {
		return storage.MerkleHash{}, err
	}
	defer f.Close()
	return storage.SplitAndStore(f, backend, splitBits), nil
}

///////////////////////////////////////////////////////////////////////////
// BackupReader

// BackupReader represents a backup that was created by BackupDir() or
// BackupDirIncremental().  It provides methods that make it possible to
// access files, directories, and file contents in the backup.
type BackupReader struct {
	root    BackupRoot
	backend storage.Backend
}

func NewBackupReader(hash storage.Hash, backend storage.Backend) (*BackupReader, error) {
	br := &BackupReader{backend: backend}
	var err error
	br.root, err = ReadRoot(hash, backend)
	return br, err
}

func (b *BackupReader) GetEntry(path string) (DirEntry, error) {
	// Split the path into components.
	s := strings.Split(filepath.Clean(path), "/")
	if len(s) > 1 && s[0] == "" {
		// Absolute path.
		s = s[1:]
	}
	return b.lookupEntry(b.root.Dir, s)
}

func (b *BackupReader) lookupEntry(e DirEntry, path []string) (DirEntry, error) {
	if len(path) == 0 || path[0] == "" {
		return e, nil
	}

	if !e.IsDir() {
		return DirEntry{}, errors.New("not a directory")
	}

	// Get the entries in the directory and look for one that matches the
	// first component of the path.
	entries := readDirEntries(e.Hash, b.backend)
	for _, entry := range entries {
		if entry.Name == path[0] {
			// Success; onward to the next path component.
			return b.lookupEntry(entry, path[1:])
		}
	}
	return DirEntry{}, errors.New("path not found")
}

func (b *BackupReader) ReadFileContents(path string) (io.ReadCloser, error) {
	e, err := b.GetEntry(path)
	if err != nil {
		return nil, err
	}
	return e.GetContentsReader(nil, b.backend)
}

func (b *BackupReader) Restore(backupPath string, dest string) error {
	entry, err := b.GetEntry(backupPath)
	if err != nil {
		return fmt.Errorf("%s: %s", backupPath, err.Error())
	}

	switch {
	case entry.IsDir():
		// We want multiple storage accesses to be in flight during restore
		// in case we're going over the network and would like to hide
		// latency.  Limit the number using the sem chan, though, so that
		// we don't hit issues with rate limits.
		ctx := &parallelContext{
			sem:          make(chan bool, 16),
			restoredDirs: make(map[string]DirEntry)}
		ctx.wg.Add(1)
		go b.restoreDir(ctx, entry, dest)
		log.Debug("start wait")
		ctx.wg.Wait()
		log.Debug("done wait")

		// Set the directory mode and modification times only after all of
		// the files have been restored. (Thus, if the mode is read-only,
		// that won't inhibit creating files during the restore, and the
		// modification times will be the stored ones, not the current
		// time, due to files being written to the directory during
		// restore.)
		for name, entry := range ctx.restoredDirs {
			log.CheckError(os.Chmod(name, entry.Mode))
			log.CheckError(os.Chtimes(name, entry.ModTime, entry.ModTime))
		}
	case entry.IsFile():
		b.restoreFile(nil, entry, dest)
	case entry.IsSymLink():
		b.restoreSymLink(entry, dest)
	default:
		return fmt.Errorf("%s: unexpected file type", backupPath)
	}
	return nil
}

type parallelContext struct {
	wg  sync.WaitGroup
	sem chan bool
	// Protects restoredDirs
	mu           sync.Mutex
	restoredDirs map[string]DirEntry
}

func (b *BackupReader) restoreDir(ctx *parallelContext, entry DirEntry, destdir string) {
	log.CheckError(os.Mkdir(destdir, 0700))

	// Limit parallelism to the number of elements buffered in the chan.  A
	// non-nil ctx is required here, unlike restoreFile.
	ctx.sem <- true
	defer func() { <-ctx.sem; ctx.wg.Done() }()

	log.Debug("%s: restoring directory", destdir)

	ctx.mu.Lock()
	// Create a new DirEntry that only stores the information we need at
	// the end; if we stored all of entry including the entries inside the
	// directory and the contents, GC would be inhibited unnecessarily.
	ctx.restoredDirs[destdir] = DirEntry{ModTime: entry.ModTime, Mode: entry.Mode}
	ctx.mu.Unlock()

	entries := readDirEntries(entry.Hash, b.backend)

	for _, e := range entries {
		path := filepath.Join(destdir, e.Name)
		switch {
		case e.IsFile():
			ctx.wg.Add(1)
			go b.restoreFile(ctx, e, path)
		case e.IsDir():
			ctx.wg.Add(1)
			go b.restoreDir(ctx, e, path)
		case e.IsSymLink():
			b.restoreSymLink(e, path)
		default:
			log.Fatal("Entry with invalid type was backed up: %+v", entry)
		}
	}
}

func (b *BackupReader) restoreFile(ctx *parallelContext, e DirEntry, path string) {
	// There are two limits to rate limit file restores: in addition to not
	// hammering on the storage backend, we also want to limit the number
	// of open files.
	var sem chan bool
	if ctx != nil {
		sem = ctx.sem
		ctx.sem <- true
		defer func() { <-ctx.sem; ctx.wg.Done() }()
	}

	log.Debug("%s: restoring file", path)

	// Create the file and set its permissions.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	log.CheckError(err)

	rc, err := e.GetContentsReader(sem, b.backend)
	_, err = io.Copy(f, rc)
	log.CheckError(err)
	log.CheckError(rc.Close())

	// Clean up.
	log.CheckError(f.Close())

	log.CheckError(os.Chmod(path, e.Mode))
	log.CheckError(os.Chtimes(path, e.ModTime, e.ModTime))
}

func (b *BackupReader) restoreSymLink(e DirEntry, path string) {
	// No need to rate-limit here.
	log.Debug("%s: restoring symlink", path)
	log.CheckError(os.Symlink(string(e.Contents), path))
}

func (b *BackupReader) Fsck() {
	// We don't need the restoredDirs map here.
	ctx := &parallelContext{sem: make(chan bool, 16)}
	ctx.wg.Add(1)
	go b.fsck(ctx, b.root.Dir)
	ctx.wg.Wait()
}

func (b *BackupReader) fsck(ctx *parallelContext, entry DirEntry) {
	ctx.sem <- true
	defer func() { <-ctx.sem; ctx.wg.Done() }()

	switch {
	case entry.IsFile():
		if entry.Contents == nil && entry.Size > 0 {
			// The file contents were stored via SplitAndStore();
			// make sure we have blobs for all of the hashes that
			// represent it.
			entry.Hash.Fsck(b.backend)
		}
	case entry.IsDir():
		entries := readDirEntries(entry.Hash, b.backend)
		ctx.wg.Add(len(entries))
		for _, e := range entries {
			go b.fsck(ctx, e)
		}
	case entry.IsSymLink():
		// Do nothing.
	default:
		log.Fatal("unexpected entry type: %+v", entry)
	}
}
