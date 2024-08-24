// rdso/rdso_test.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package rdso

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"testing"
	"time"
)

func TestE2E(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("Seed = %d", seed)
	rand.Seed(seed)

	// Make a buffer full of random bytes.
	buf := make([]byte, rand.Intn(32*1024*1024))
	t.Logf("Length %d", len(buf))
	_, _ = rand.Read(buf)
	origBuf := dupe(buf)

	nShards := 1 + rand.Intn(24)
	nParity := 1 + rand.Intn(8)
	hashRate := 1 << uint(10+rand.Intn(10))
	t.Logf("%d data shards, %d parity, %d hash rate", nShards, nParity, hashRate)

	// Encode the bytes.
	var rs bytes.Buffer
	err := Encode(bytes.NewReader(buf), int64(len(buf)), &rs, nShards, nParity, hashRate)
	if err != nil {
		t.Fatalf("%s", err)
	}
	origRs := dupe(rs.Bytes())

	// The initial check should pass!
	err = Check(bytes.NewReader(buf), bytes.NewReader(rs.Bytes()), nil)
	if err != nil {
		t.Fatalf("Error %+v on initial check", err)
	}

	// Introduce as many errors as possible to the data and the encoded
	// bytes while still being able to recover.
	nErrors := nParity
	t.Logf("Introducing %d errors", nErrors)

	de := rand.Intn(nErrors)
	corrupt(buf, de, nShards*hashRate)
	if err = corruptRS(origBuf, rs.Bytes(), nErrors-de, t); err != nil {
		t.Fatalf("%s", err)
	}

	// Make sure that the check fails now.
	err = Check(bytes.NewReader(buf), bytes.NewReader(rs.Bytes()), nil)
	if err == nil {
		t.Fatalf("CheckFile of corrupted file didn't fail?")
	} else if err != ErrFileCorrupt {
		t.Fatalf("%s", err)
	}

	// Restore it.
	var restored, restoredRs bytes.Buffer
	if err = Restore(bytes.NewReader(buf), bytes.NewReader(rs.Bytes()),
		int64(len(buf)), &restored, &restoredRs, nil); err != nil {
		t.Fatalf("%s", err)
	}

	// Make sure that the recovered data matches the original
	if !bytes.Equal(origBuf, restored.Bytes()) {
		t.Errorf("original bytes don't match restored")
	}
	if !bytes.Equal(origRs, restoredRs.Bytes()) {
		t.Errorf("original rs bytes don't match restored")
	}
}

func dupe(b []byte) []byte {
	r := make([]byte, len(b))
	copy(r, b)
	return r
}

// Corrupt the given data.
func corrupt(b []byte, n int, sz int) {
	// Take advatage of the fact that we know how the file is segmented and
	// sharded; introduce n errors in each segment.
	for len(b) > 0 {
		if sz > len(b) {
			// Last time through
			sz = len(b)
		}

		for i := 0; i < n; i++ {
			offset := rand.Intn(sz)
			v := b[offset]
			delta := 1 + rand.Intn(254)
			b[offset] = byte((int(v) + delta) % 255)
		}
		b = b[sz:]
	}
}

// Corrupt n random bytes of the given .rs file, being careful to not
// clobber any of the hashes.
func corruptRS(data, rs []byte, n int, t *testing.T) error {
	if n == 0 {
		return nil
	}

	var w bytes.Buffer
	enc := gob.NewEncoder(&w)
	first := true

	err := forEachSegment(bytes.NewReader(data), bytes.NewReader(rs), nil,
		func(h rsFileHeader, hashes []hash, shards [][]byte) error {
			if first {
				if err := enc.Encode(h); err != nil {
					return err
				}
				first = false
			}

			// Add n errors in the current segment
			parity := shards[h.NDataShards:]
			for i := 0; i < n; i++ {
				target := rand.Intn(len(parity))
				off := rand.Intn(len(parity[target]))
				parity[target][off] += byte(1 + rand.Intn(254))
			}

			// In any case, write out the segment.
			return enc.Encode(rsFileSegment{hashes, parity})
		})
	if err != nil {
		return err
	}
	copy(rs, w.Bytes())

	return nil
}
