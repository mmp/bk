// cmd/bk/fuse.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package main

/*
FS is BackupReader, augmented with Root() method

DirEntry implements Attr()
Lookup(name): easy: just 1 path component
ReadDirAll(): on top of readDir Entries
ReadAll() via GetContentsReader
*/
