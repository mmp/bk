// cmd/bk/readme.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package main

var readmeText = `

This document is an attempt to document the way that bk backs up data in
sufficient detail so that (if ever necessary), it's possible to restore a
backup from a bk repository even without the existing bk source code. We'll
proceed in bottom-up fashion from the low-level storage systems up to the
backup-specific representations built on top of them.

# Storage Format (both on-disk and GCS)

The main task of the bk storage systems is to take chunks of data, store
them safely, and return hashes that identify them. bk uses SHAKE256 to hash
chunks (though that doesn't matter for restoring) and then uses 32 bytes
worth of hash for each chunk.

The packs/ directory stores pack files, which store a series of blobs.
Each blob is stored starting with the 4-byte string "BL0B". Next is the
length of the chunk stored using go's binary.PutVarint followed by the
chunk's data.

The filenames of blob files are arbitrary. Each blob file has a
corresponding index file in the indices/ directory. Index files allow us to
efficiently find out which blob hashes are already stored and where the
corresponding blobs are in pack files.

Each index in an index file starts with the magic number "Idx2", then 32
bytes of the corresponding chunk's hash, the offset in the pack file where
the chunk starts and the length of the chunk (both also encoded using
binary.PutVaring).

Note that the index files can be reconstructed from the pack file contents
alone.

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

# Compression and Encryption

Chunks in pack files are compressed using gzip if doing so makes them
smaller. The first byte of each chunk is one if it was compressed and is
zero if it's uncompressed.

Chunks may be encrypted as well; encryption is appied *after* gzip
compression (otherwise compression would be useless) and so decryption
should be applied before decompression during restore.  If encryption is
used, each chunk starts with a random 16 byte initialization vector. Chunk
data is encrypted using the go standard library's implementation of the AES
encryption algorithm.

To get the decryption key: First, the file metadata/encrypt.txt has four
hex-encoded values.  In order: a salt, the hash of the passphrase, the
encrypted key, and the IV used to encrypt the encryption key.

Given the passphrase from the user, a 64-byte derived key is computed using
65536 rounds of pbkdf2:

	derivedKey := pbkdf2.Key([]byte(passphrase), salt, 65536, 64, sha256.New)

The first 32 bytes of the result should match the passphrase hash in
encrypt.txt.  The last 32 bytes give the decryption key to use to decrypt
the encrypted key from encrypt.txt. (Again, using go's AES implementation.)

# Backing up bistreams

Each bitstream backup (as done using "bk savebits") has an associated file
in the metadata directory of the form metadata/bits-*. That file stores a
the hash for the root of a Merkle hash tree, where the first 32 bytes give
the hash of a chunk in the storage system and the last bytes gives the
depth of the Merkle hash tree--if zero, then the associated hash refers to
the actual data for the stored bitstream; if one, then the data associated
with the hash is itself a series of 32-byte hashes; reading and
concatenating their data gives the bitstream, and so forth.

# Backing up directory hierarchies

Each file system backup has a 32-byte hash stored in a metadata/backups-*
file. The data associated with that hash is in turn a BackupRoot,
represented as

type BackupRoot struct {
	Dir  DirEntry
	Time time.Time
}

and encoded using go's "gob" encode. Dir refers to the root of the
hierarchy and is a gob-encoded instance of this structure:

type DirEntry struct {
	// Just the file name, not its full path.
	Name string
	// If non-nil, stores the file's contents. Empty files have nil
	// contents and an invalid hash, so it's important to check the Size
	// before trying to make use of either.  Symlink targets are always
	// stored here.
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

The MerkleHash values are encoded as described in "Backing up bitstreams."

`
