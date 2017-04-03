// storage/packidx.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// TODO: two byte magic number for Idx to save space?
var IdxMagic = [4]byte{'I', 'd', 'x', '2'}
var BlobMagic = [4]byte{'B', 'L', '0', 'B'}

// BlobPacker takes (hash, chunk) pairs and converts them to the
// representation to be stored in pack and index files.  It tracks the
// current size of the pack file, so a new BlobPacker should be used for
// each new pair of index/pack files created.
type BlobPacker struct {
	packFileSize int64
}

/*
File format specs:
- Pack file: for each chunk, stores BlobMagic, the length of the chunk encoded
  as a varint, and then the chunk contents.
- Index file: for each chunk, stores IdxMagic, the hash, and then the offset
  into the pack file and the length of the chunk, both encoded as varints.

Note: index files can be recreated from pack files alone.
*/

// Given a chunk and its hash, Pack returns the bytes to append to the
// current index and back files to store the chunk.
func (p *BlobPacker) Pack(h Hash, chunk []byte) (idx, pack []byte) {
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
	ni += binary.PutVarint(idx[ni:], p.packFileSize)
	ni += binary.PutVarint(idx[ni:], int64(len(pack)))
	idx = idx[:ni]

	p.packFileSize += int64(len(pack))
	return
}

func fsckPackFile(r io.Reader, allHashes map[Hash]struct{}) {
	err := DecodePackFile(r, func(chunk []byte) {
		hash := HashBytes(chunk)
		if _, ok := allHashes[hash]; !ok {
			log.Error("%s: hash found in pack file, but not in index", hash)
		}
	})
	if err != nil {
		log.Error("%s", err)
	}
}

///////////////////////////////////////////////////////////////////////////
// ChunkIndex maintains an index from hashes to the locations of blobs
// in pack files.
type ChunkIndex struct {
	hashToLoc map[Hash]blobLoc
	nameToId  map[string]int
	idToName  []string
}

// Internal representation (for compactness).
type blobLoc struct {
	id     int
	offset int64
	length int64
}

// External representation returned to callers.
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

	id, ok := c.nameToId[packName]
	if !ok {
		id = len(c.idToName)
		c.nameToId[packName] = id
		c.idToName = append(c.idToName, packName)
	}

	c.hashToLoc[hash] = blobLoc{id, offset, length}
}

// Takes entire contents of index file, associates entries with the given
// pack file name.
func (c *ChunkIndex) AddIndexFile(packName string, idx []byte) error {
	for len(idx) > 0 {
		if bytes.Compare(idx[:len(IdxMagic)], IdxMagic[:]) != 0 {
			return ErrIndexMagicWrong
		}
		idx = idx[len(IdxMagic):]

		var hash Hash
		n := copy(hash[:], idx)
		if n < HashSize {
			return ErrPrematureEndOfData
		}

		offset, nvar := binary.Varint(idx[n:])
		if nvar <= 0 {
			return fmt.Errorf("varint: returned %d", nvar)
		}
		n += nvar

		length, nvar := binary.Varint(idx[n:])
		if nvar <= 0 {
			return fmt.Errorf("varint: returned %d", nvar)
		}
		n += nvar

		c.AddSingle(hash, packName, offset, length)

		idx = idx[n:]
	}
	return nil
}

func (c *ChunkIndex) Lookup(hash Hash) (BlobLocation, error) {
	loc, ok := c.hashToLoc[hash]
	if !ok {
		return BlobLocation{}, ErrHashNotFound
	}

	return BlobLocation{c.idToName[loc.id], loc.offset, loc.length}, nil
}

func (c *ChunkIndex) Hashes() map[Hash]struct{} {
	m := make(map[Hash]struct{})
	for h := range c.hashToLoc {
		var empty struct{}
		m[h] = empty
	}
	return m
}

// DecodeBlob takes a blobread from a pack file (as per the specs in
// BlobLocation) and returns the chunk stored in that blob.
func DecodeBlob(blob []byte) (chunk []byte, err error) {
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
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		f(chunk)
	}
}

// Returns the chunk and nil on success, a nil chunk an io.EOF on a "clean"
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
