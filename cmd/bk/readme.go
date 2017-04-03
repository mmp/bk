// cmd/bk/readme.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package main

var readmeText = `

// TODO: update this to actually be correct, then finish writing it.

This document is an attempt to document the way that bk backs up data in
sufficient detail so that (if ever necessary), it's possible to restore a
backup from a bk repository even without the existing bk source code. We'll
proceed in bottom-up fashion from the low-level storage systems up to the
backup-specific representations built on top of them.

# On Disk Storage

The main task of the bk storage systems is to take chunks of data, store
them safely, and return hashes that identify them. bk uses SHAKE256 to hash
blobs and then takes 32 bytes worth of hash for each chunk.

blobs are stored in the packs/ directory, which stores pack files, which
start with the 4-byte string "P@ck" and then the file format version number
stored as a 4-byte little endian integer. (The only valid version number so
far is 1.)  Blobs follow.

Each blob is stored starting with the 4-byte string "Bl0b". Next is the
64-byte hash and then the length of the blob, stored as a little-endian
64-bit integer. The actual blob data follows.

The filenames of blob files are arbitrary. Each blob file has a
corresponding index file in the indices/ directory. Index files allow us to
efficiently find out which blob hashes are already stored and where the
corresponding blobs are in pack files.

Index files start with "Idxf" and the file version (again as a little
endian 32-bit integer, only 1 is valid). Blob references follow. Each one
is first the 64-byte hash for the blob, followed by the blob's offset in
the pack file encoded as a little-endian 64-bit integer.

# Reed-Solomon encoding

All files stored on disk are coded with Reed-Solomon encoding. The
Reed-Solomon parity information is stored in a .rs file for each regular
file.  The .rs files are based on the Go "gob" encoding package; they just
store the following structure:

const HashSize = 64
type Hash [HashSize]byte

type ReedSolomonFile struct {
	// Size of the original file
	FileSize                   int64
	NDataShards, NParityShards int
	HashRate                   int64
	Hashes                     [][]Hash // First the data hashes, then the parity hashes.
	ParityShards               [][]byte
}


# Compression

# Encryption

# Splitting

# Backing up directory hierarchies
`
