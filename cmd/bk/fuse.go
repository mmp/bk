// cmd/bk/fuse.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package main

// Additional infrastructure to allow accessing backups via FUSE.

import (
	"github.com/mmp/bk/storage"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"golang.org/x/net/context"
)

type namedBackup struct {
	name string
	time time.Time
	br   *BackupReader
}

// mountFUSE takes the given namedBackups and exports a FUSE filesystem
// where the first three levels of the directory hierarchy are the backup
// name, the yymmdd, and the hhmmss of when the snapshot was taken. Below
// that is the directory hierarchy in the backup.
func mountFUSE(dir string, nb []namedBackup) {
	conn, err := fuse.Mount(
		dir,
		fuse.FSName("bkfs"),
		fuse.Subtype("bkfs"),
		fuse.VolumeName("backups"),
		fuse.ReadOnly(),
	)
	log.CheckError(err)
	defer conn.Close()

	root := createPseudoHierarchy(nb)
	err = fs.Serve(conn, root)
	log.CheckError(err)

	<-conn.Ready
	if err := conn.MountError; err != nil {
		log.CheckError(err)
	}
}

// Implements various FUSE interfaces for the top few levels of
// the hierarchy: backup_name/yymmdd/hhmmss.
type pseudoDir struct {
	name string
	// Each pseudoDir either has 1+ subdirectories in entries, or a non-nil
	// *BackupReader (at the node before the actual backup starts).
	entries []*pseudoDir
	br      *BackupReader
}

// Given an array of named backups of the form "backup_name-yymmdd-hhmmss",
// create the corresponding *pseudoDir hierarchy.
func createPseudoHierarchy(nb []namedBackup) *pseudoDir {
	var root pseudoDir
	for _, b := range nb {
		comps := strings.Split(b.name, "-")
		log.Check(len(comps) == 3)
		pseudoAddRecursive(&root, comps, b)
	}
	return &root
}

func pseudoAddRecursive(pd *pseudoDir, comps []string, nb namedBackup) {
	if len(comps) == 0 {
		// Reached the hhmmss directory; below here, it's all provided by
		// the BackupReader.
		pd.br = nb.br
		return
	}

	// If we already have a pseudoDir for the current path component,
	// proceed recursively with it.
	for _, e := range pd.entries {
		if e.name == comps[0] {
			pseudoAddRecursive(e, comps[1:], nb)
			return
		}
	}
	// Otherwise add the component to the current pseudoDir and recurse.
	pd.entries = append(pd.entries, &pseudoDir{name: comps[0]})
	pseudoAddRecursive(pd.entries[len(pd.entries)-1], comps[1:], nb)
}

// Root() should only be called with the root node passed to fs.Serve;
// since pseudoDir also implements the additional Node and Handle
// interfaces for a directory entry, we can just return it directly.
func (pd *pseudoDir) Root() (fs.Node, error) {
	return pd, nil
}

func (pd *pseudoDir) Attr(ctx context.Context, a *fuse.Attr) error {
	// All pseudorDirs are directories.
	a.Mode = os.ModeDir | 0500
	return nil
}

// Implements fuse.fs.NodeStringLookuper interface (OMGWTFBBQ naming)
func (pd *pseudoDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	for _, entry := range pd.entries {
		if entry.name != name {
			continue
		}
		if entry.br != nil {
			// Hand-off to dirEntryBackend for subsequent levels down the
			// hierarchy.
			return &dirEntryBackend{entry.br.root.Dir, entry.br.backend}, nil
		} else {
			return entry, nil
		}
	}
	return nil, fuse.ENOENT
}

// Implements fuse.fs.HandleReadDirAller
func (pd *pseudoDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var de []fuse.Dirent
	for _, entry := range pd.entries {
		de = append(de, fuse.Dirent{Name: entry.name, Type: fuse.DT_Dir})
	}
	return de, nil
}

///////////////////////////////////////////////////////////////////////////

// Helper class that agglomerates DirEntry and a storage backend that
// can give us its children.
type dirEntryBackend struct {
	DirEntry
	backend storage.Backend
}

func (e *dirEntryBackend) Attr(ctx context.Context, a *fuse.Attr) error {
	if e.IsFile() {
		a.Size = uint64(e.Size)
	}
	a.Mode = e.Mode
	a.Mtime = e.ModTime
	return nil
}

// Implements fuse.fs.NodeStringLookuper interface (OMGWTFBBQ naming)
func (e *dirEntryBackend) Lookup(ctx context.Context, name string) (fs.Node, error) {
	log.Check(e.IsDir())
	entries := readDirEntries(e.Hash, e.backend)
	for _, entry := range entries {
		if entry.Name == name {
			return &dirEntryBackend{entry, e.backend}, nil
		}
	}
	return nil, fuse.ENOENT
}

// Implements fuse.fs.HandleReadDirAller
func (e *dirEntryBackend) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var dirents []fuse.Dirent
	for _, entry := range readDirEntries(e.Hash, e.backend) {
		de := fuse.Dirent{Name: entry.Name}
		switch {
		case e.IsDir():
			de.Type = fuse.DT_Dir
		case e.IsFile():
			de.Type = fuse.DT_File
		case e.IsSymLink():
			de.Type = fuse.DT_Link
		default:
			log.Fatal("Unhandled DirEntry type: %+v", e)
		}
		dirents = append(dirents, de)
	}
	return dirents, nil
}

// Implements fuse.fs.HandleReadAller
func (e *dirEntryBackend) ReadAll(ctx context.Context) ([]byte, error) {
	r, err := e.GetContentsReader(nil, e.backend)
	if err != nil {
		return nil, err
	}

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return b, err
	}
	return b, r.Close()
}
