// rdso/rdso.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

// Simple APIs to apply Reed-Solomon encoding to files, based on
// github.com/klauspost/reedsolomon. Provides facilities to check the
// integrity of encoded files and to recover corrupt files.

package rdso

import (
	"encoding/gob"
	"github.com/klauspost/reedsolomon"
	u "github.com/mmp/bk/util"
	"golang.org/x/crypto/sha3"
	"io"
	"os"
)

// HashSize is the number of bytes in the hash values returned to
// represent blobs of data.
const HashSize = 64

// Hash encodes a fixed-size secure hash of a collection of bytes.
type Hash [HashSize]byte

// HashBytes computes the SHAKE256 hash of the given byte slice.
func HashBytes(b []byte) Hash {
	var h Hash
	sha3.ShakeSum256(h[:], b)
	return h
}

type ReedSolomonFile struct {
	// Size of the original file
	FileSize                   int64
	NDataShards, NParityShards int
	HashRate                   int64
	Hashes                     [][]Hash // First the data hashes, then the parity hashes.
	ParityShards               [][]byte
}

func EncodeFile(fn, rsfn string, nDataShards int, nParityShards int,
	hashRate int64) error {
	rs := ReedSolomonFile{
		NDataShards:   nDataShards,
		NParityShards: nParityShards,
		HashRate:      hashRate,
	}

	// Read the file from disk and shard it.
	var err error
	var dataShards [][]byte
	dataShards, rs.FileSize, err = readAndShardFile(fn, nDataShards)
	if err != nil {
		return err
	}

	// Allocate storage for the parity shards.
	for i := 0; i < nParityShards; i++ {
		rs.ParityShards = append(rs.ParityShards,
			make([]byte, len(dataShards[0])))
	}

	// Reed-Solomon encode the sharded file.
	enc, err := reedsolomon.New(nDataShards, nParityShards)
	if err != nil {
		return err
	}
	allShards := append(dataShards, rs.ParityShards...)
	err = enc.Encode(allShards)
	if err != nil {
		return err
	}

	// Sanity check the results.
	ok, err := enc.Verify(allShards)
	if !ok || err != nil {
		panic("verify failed")
	}

	// Compute the hashes.
	for _, s := range dataShards {
		rs.Hashes = append(rs.Hashes, hash(shard(s, hashRate)))
	}
	for _, s := range rs.ParityShards {
		rs.Hashes = append(rs.Hashes, hash(shard(s, hashRate)))
	}

	// Write the .rs file
	fout, err := os.Create(rsfn)
	if err != nil {
		return err
	}
	genc := gob.NewEncoder(fout)
	err = genc.Encode(rs)
	if err != nil {
		return err
	}
	return fout.Close()
}

// Shards into first nshards
func readAndShardFile(fn string, nshards int) (shards [][]byte,
	size int64, err error) {
	f, err := os.Open(fn)
	if err != nil {
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return
	}
	size = fi.Size()

	shardSize := (fi.Size() + int64(nshards) - 1) / int64(nshards)
	// Allocate extra space so all shards can be the same size.
	buf := make([]byte, int64(nshards)*shardSize)

	// Read the file contents into the buffer.
	_, err = io.ReadFull(f, buf[:fi.Size()])
	if err != nil {
		return
	}

	// Zero pad the end of the last shard
	buf = buf[:cap(buf)]

	shards = shard(buf, shardSize)

	return
}

func shard(b []byte, size int64) (s [][]byte) {
	for {
		if int64(len(b)) > size {
			s = append(s, b[:size])
			b = b[size:]
		} else {
			s = append(s, b)
			return
		}
	}
}

func hash(b [][]byte) (hashes []Hash) {
	for _, s := range b {
		hashes = append(hashes, HashBytes(s))
	}
	return
}

func CheckFile(fn, rsfn string, log *u.Logger) error {
	return checkOrRestore(fn, rsfn, log, false)
}

func RestoreFile(fn, rsfn string, log *u.Logger) error {
	return checkOrRestore(fn, rsfn, log, true)

}

func checkOrRestore(fn, rsfn string, log *u.Logger, restore bool) error {
	// Read the .rs file for the data file.
	rs, err := readRsFile(rsfn)
	if err != nil {
		return err
	}

	// Read and shard the data file.
	dataShards, _, err := readAndShardFile(fn, rs.NDataShards)
	if err != nil {
		return err
	}

	// First shard as for R-S, then shard for the hash chunk size
	var allShards [][][]byte
	for _, s := range dataShards {
		allShards = append(allShards, shard(s, rs.HashRate))
	}
	for _, s := range rs.ParityShards {
		allShards = append(allShards, shard(s, rs.HashRate))
	}

	// Loop over the hash chunks
	errors := 0
	nHashChunks := len(allShards[0]) // == len(allShards[*])
	for hc := 0; hc < nHashChunks; hc++ {
		for s := 0; s < len(allShards); s++ {
			if HashBytes(allShards[s][hc]) != rs.Hashes[s][hc] {
				if log != nil {
					if s < len(dataShards) {
						if restore {
							log.Warning("%s: data shard %d hash %d mismatch\n",
								fn, s, hc)
						} else {
							log.Error("%s: data shard %d hash %d mismatch\n",
								fn, s, hc)
						}
					} else {
						if restore {
							log.Warning("%s: parity shard %d hash %d mismatch\n",
								fn, s-len(dataShards), hc)
						} else {
							log.Error("%s: parity shard %d hash %d mismatch\n",
								fn, s-len(dataShards), hc)
						}
					}
				}
				errors++
				// nil it out (in case we're going to try and recover)
				allShards[s][hc] = nil
			}
		}
	}

	// See if we need to recover (and are supposed to.)
	if !restore || errors == 0 {
		return nil
	}

	// Try to recover the file.
	enc, err := reedsolomon.New(rs.NDataShards, rs.NParityShards)
	if err != nil {
		return err
	}

	for hc := 0; hc < nHashChunks; hc++ {
		// Recover this chunk, if needed.
		missing := 0
		var recon [][]byte
		for _, shard := range allShards {
			recon = append(recon, shard[hc])
			if shard[hc] == nil {
				missing++
			}
		}
		if missing > 0 {
			err = enc.Reconstruct(recon)
			if err != nil {
				return err
			}
		}

		for s := 0; s < len(dataShards); s++ {
			copy(dataShards[s][int64(hc)*rs.HashRate:], recon[s])
		}
	}

	// Write out new file
	f, err := os.Create(fn + ".recovered")
	if err != nil {
		return nil
	}
	w := &limitedWriter{f, rs.FileSize}
	for _, shard := range dataShards {
		_, err = w.Write(shard)
		if err != nil {
			return nil
		}
	}

	return f.Close()
}

type limitedWriter struct {
	W io.Writer
	N int64
}

func (w *limitedWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > w.N {
		data = data[:w.N]
	}
	n, err := w.W.Write(data)
	w.N -= int64(n)
	return n, err
}

func readRsFile(fn string) (ReedSolomonFile, error) {
	var rs ReedSolomonFile
	f, err := os.Open(fn)
	if err != nil {
		return rs, err
	}
	d := gob.NewDecoder(f)
	err = d.Decode(&rs)
	if err != nil {
		return rs, err
	}
	f.Close()
	return rs, nil
}
