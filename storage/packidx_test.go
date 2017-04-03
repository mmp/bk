// storage/packidx_test.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package storage

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestPacker(t *testing.T) {
	var idx, pack []byte
	var blobs [][]byte
	var packer BlobPacker

	var sumLen []int
	sumLen = append(sumLen, 0)

	const nChunks = 1000
	for i := 0; i < nChunks; i++ {
		b := make([]byte, rand.Intn(65536))
		rand.Read(b)
		blobs = append(blobs, b)
		sumLen = append(sumLen, sumLen[len(sumLen)-1]+len(b))

		i, p := packer.Pack(HashBytes(b), b)

		idx = append(idx, i...)
		pack = append(pack, p...)
	}

	var index ChunkIndex
	const packName = "foobar.pack"
	err := index.AddIndexFile(packName, idx)
	if err != nil {
		t.Fatalf("Add: %+v", err)
	}

	for i := 0; i < nChunks; i++ {
		loc, err := index.Lookup(HashBytes(blobs[i]))
		if err != nil {
			t.Errorf("%d: %+v", i, err)
		}
		if loc.PackName != packName {
			t.Errorf("Got pack name %s, not %s", loc.PackName, packName)
		}

		b := pack[loc.Offset:]
		b = b[:loc.Length]
		chunk, err := DecodeBlob(b)
		if err != nil {
			t.Errorf("%d: decode blob error: %+v", i, err)
		} else if len(chunk) != len(blobs[i]) {
			t.Errorf("%d: Got size %d, expected %d", i, len(chunk), len(blobs[i]))
		} else if bytes.Compare(chunk, blobs[i]) != 0 {
			t.Errorf("%d: chunk compare failed", i)
		}
	}
}
