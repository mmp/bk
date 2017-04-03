// storage/split.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package storage

import (
	"bufio"
	"bytes"
	"io"
)

type MerkleHash struct {
	Hash  Hash
	Level uint8
}

func MerkleFromSingle(hash Hash) MerkleHash {
	return MerkleHash{hash, 0}
}

func (sh *MerkleHash) Bytes() []byte {
	var b []byte
	b = append(b, sh.Hash[:]...)
	b = append(b, sh.Level)
	return b
}

func NewMerkleHash(b []byte) MerkleHash {
	var sh MerkleHash
	n := copy(sh.Hash[:], b)
	sh.Level = b[n]
	return sh
}

func DecodeMerkleHash(r io.Reader) (sh MerkleHash) {
	var b [HashSize + 1]byte
	_, err := io.ReadFull(r, b[:])
	log.CheckError(err)
	return NewMerkleHash(b[:])
}

func (h *MerkleHash) NewReader(sem chan bool, backend Backend) io.ReadCloser {
	hashes := []Hash{h.Hash}
	for level := h.Level; level > 0; level-- {
		r := NewHashesReader(hashes, sem, backend)
		hashes = readHashes(r)
		log.CheckError(r.Close())
	}
	return NewHashesReader(hashes, sem, backend)
}

func (h *MerkleHash) Fsck(backend Backend) {
	hashes := []Hash{h.Hash}
	for level := h.Level; level > 0; level-- {
		for _, hash := range hashes {
			if !backend.HashExists(hash) {
				log.Error("%s: hash not found in storage.", hash)
			}
		}
		r := NewHashesReader(hashes, nil, backend)
		hashes = readHashes(r)
		log.CheckError(r.Close())
	}
}

func readHashes(r io.Reader) (hashes []Hash) {
	for {
		var hash Hash
		_, err := io.ReadFull(r, hash[:])
		if err == io.EOF {
			return
		}
		log.CheckError(err)
		hashes = append(hashes, hash)
	}
}

///////////////////////////////////////////////////////////////////////////

// Split the bytes of the given io.Reader using a rolling checksum into chunks
// of size (on average) 1<<splitBits.  Return the hash for the root of a Merkle
// tree that identifies the data stored in the given storage backend.
func SplitAndStore(r io.Reader, backend Backend, splitBits uint) MerkleHash {
	// Wrap the reader with a buffered reader if it isn't buffered already
	// (as is the case for, e.g. stdin).  This is required for decent
	// performance in the the splitter code, which needs to process the
	// input a byte at a time.
	br, ok := r.(io.ByteReader)
	if !ok {
		br = bufio.NewReader(r)
	}

	// Put the bits from the reader in storage and get the hashes that
	// reconstruct them.
	hs := NewHashSplitter(splitBits)
	hashes := splitAndStoreMerkleTree(br, backend, hs)

	// Now, continue to split and store the hash bytes until we're down to
	// a single hash; that's the final identifier for the provided bits
	// that we'll return.
	for level := uint8(0); ; level++ {
		if len(hashes) == 1 {
			return MerkleHash{hashes[0], level}
		}
		log.Check(level < 10)

		// Construct the buffer to store.
		var buf []byte
		for _, h := range hashes {
			buf = append(buf, h[:]...)
		}

		hs.Reset()
		hashes = splitAndStoreMerkleTree(bytes.NewBuffer(buf), backend, hs)
	}
}

func splitAndStoreMerkleTree(r io.ByteReader, backend Backend, hs *HashSplitter) (hashes []Hash) {
	for {
		// Get the next blob of data from the input stream.
		blob := hs.SplitFromReader(r)
		if len(blob) == 0 {
			return // done
		}
		hashes = append(hashes, backend.Write(blob))
		hs.Reset()
	}
}

///////////////////////////////////////////////////////////////////////////
// Rolling checksum stuff from bup...

// The lowest bits seem to be most useful; splitting based on, say, 4 bits
// in the middle is fiddly, especially when it spans the 16th
// bit.
type HashSplitter struct {
	splitBits uint
	s1, s2    uint32
	window    [splitWindowSize]byte
	wofs      int
	count     int
}

const splitterCharOffset = 31
const splitWindowBits = 6
const splitWindowSize = 1 << splitWindowBits

func NewHashSplitter(splitBits uint) *HashSplitter {
	log.Check(splitBits >= 8 && splitBits <= 18)
	hs := &HashSplitter{
		splitBits: splitBits,
		s1:        splitWindowSize * splitterCharOffset,
		s2:        splitWindowSize * (splitWindowSize - 1) * splitterCharOffset,
	}
	return hs
}

func (hs *HashSplitter) Reset() {
	hs.s1 = splitWindowSize * splitterCharOffset
	hs.s2 = splitWindowSize * (splitWindowSize - 1) * splitterCharOffset
	hs.wofs = 0
	hs.count = 0
	for i := 0; i < splitWindowSize; i++ {
		hs.window[i] = 0
	}
}

func (hs *HashSplitter) AddByte(b byte) {
	drop := hs.window[hs.wofs]
	hs.s1 += uint32(b) - uint32(drop)
	hs.s2 += hs.s1 - (splitWindowSize * uint32(drop+splitterCharOffset))
	hs.window[hs.wofs] = b
	hs.wofs = (hs.wofs + 1) % splitWindowSize
	hs.count++
}

func (hs *HashSplitter) SplitNow() bool {
	if hs.count < 8*splitWindowSize {
		return false
	}
	digest := (hs.s1 << 16) | (hs.s2 & 0xffff)
	splitSize := 1 << hs.splitBits
	return (digest & uint32(splitSize-1)) == uint32(splitSize-1)
}

func (hs *HashSplitter) SplitFromReader(reader io.ByteReader) (ret []byte) {
	for {
		add, err := reader.ReadByte()
		if err == io.EOF {
			return
		}
		log.CheckError(err)

		hs.AddByte(add)
		ret = append(ret, add)
		if hs.SplitNow() {
			return
		}
	}
}
