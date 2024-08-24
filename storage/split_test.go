// storage/split_test.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package storage

import (
	"bytes"
	"crypto/sha1"
	"io"
	"math/rand"
	"os"
	"sync"
	"testing"
)

func TestSplitCorrectAndDistribution(t *testing.T) {
	seed := int64(os.Getpid())
	rand.Seed(seed)
	t.Logf("Seed %d", seed)

	// Make a random byte array
	const sz = 32 * 1024 * 1024
	b := make([]byte, sz+rand.Intn(sz))
	_, _ = rand.Read(b)

	// For each of a range of split bits...
	for sb := 8; sb <= 16; sb++ {
		// Slice it until we get an empty slice back
		var sliced []byte
		reader := bytes.NewReader(b)
		numSlices := 0
		hs := NewHashSplitter(uint(sb))
		for {
			slice := hs.SplitFromReader(reader)
			if len(slice) == 0 {
				break
			}
			numSlices++
			// Store the values that were returned
			sliced = append(sliced, slice...)
			hs.Reset()
		}

		// And make sure they match the original bytes!
		if !bytes.Equal(b, sliced) {
			t.Fatalf("Contents don't match.")
		}

		// Finally, also make sure we got back a reasonable number of
		// slices.
		avgLen := 1 << uint(sb)
		expectedSlices := len(b) / avgLen
		switch sb {
		// Ad-hoc correction factors since the minimum size requirement
		// skews things a bit for small split sizes.
		case 8:
			expectedSlices /= 3
		case 9:
			expectedSlices /= 2
		}
		if numSlices < expectedSlices/2 ||
			numSlices > expectedSlices*3/2 {
			t.Errorf("Got %d slices for %d bits; expected ~%d",
				numSlices, sb, expectedSlices)
		}
	}
}

type SHA1Hash [sha1.Size]byte

// Make an array, split it, make a 1-byte change, split again, make sure we
// don't get too many new chunks.
func TestSplitByteChange(t *testing.T) {
	seed := int64(os.Getpid())
	rand.Seed(seed)
	t.Logf("Seed %d", seed)

	b := make([]byte, 512*1024)
	_, _ = rand.Read(b)

	var wg sync.WaitGroup
	for _, sb := range []int{8, 13, 16, 18} {
		wg.Add(1)

		go func(sb int) {
			testByteChange(t, b, sb, seed)
			wg.Done()
		}(sb)
	}
	wg.Wait()
}

func testByteChange(t *testing.T, borig []byte, sb int, seed int64) {
	b := make([]byte, len(borig))
	copy(b, borig)

	blobs := make(map[SHA1Hash]struct{})
	rng := rand.New(rand.NewSource(seed))

	var empty struct{}
	reader := bytes.NewReader(b)
	hs := NewHashSplitter(uint(sb))

	for {
		slice := hs.SplitFromReader(reader)
		if len(slice) == 0 {
			break
		}
		blobs[sha1.Sum(slice)] = empty
		hs.Reset()
	}

	totalExtra := 0
	for i := 0; i < 1000; i++ {
		offset := rng.Intn(len(b) - 2)
		op := rng.Intn(3)
		switch op {
		case 0:
			// Change one byte
			delta := 1 + rng.Intn(254)
			b[offset] += byte(delta)
		case 1:
			// Insert a new byte
			bnew := append(b[:offset], byte(rng.Intn(255)))
			b = append(bnew, b[offset:]...)
		case 2:
			// Delete a byte
			b = append(b[:offset], b[offset+1:]...)
		}

		// Resplit and count how many new blobs we get.
		reader.Seek(io.SeekStart, 0)

		newHashes := 0
		for {
			slice := hs.SplitFromReader(reader)
			if len(slice) == 0 {
				break
			}

			hash := sha1.Sum(slice)
			if _, ok := blobs[hash]; !ok {
				blobs[hash] = empty
				newHashes++
			}

			hs.Reset()
		}

		if newHashes > 2 {
			totalExtra += newHashes - 2
		}
	}

	t.Logf("bits %d: Total of %d > 2 hashes", sb, totalExtra)
	if totalExtra > 5 {
		t.Errorf("bits %d: Total of %d > 2 hashes", sb, totalExtra)
	}
}
