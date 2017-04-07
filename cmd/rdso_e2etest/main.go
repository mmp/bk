// cmd/rdso_e2etest/main.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package main

import (
	"bytes"
	"github.com/mmp/bk/rdso"
	u "github.com/mmp/bk/util"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
)

func main() {
	log := u.NewLogger(true /*verbose*/, false /*debug*/)

	seed := int64(os.Getpid())
	log.Verbose("Seed = %d", seed)
	rand.Seed(seed)

	// Make a file full of random bytes.
	len := 64 + rand.Intn(128*1024*1024)
	log.Verbose("File length %d", len)
	buf := make([]byte, len)
	_, _ = rand.Read(buf)

	f, err := ioutil.TempFile("", "rdso_e2e")
	name := f.Name()
	defer os.Remove(name)
	if err != nil {
		log.Fatal("%s", err)
	}

	_, err = io.Copy(f, bytes.NewReader(buf))
	if err != nil {
		log.Fatal("%s", err)
	}

	err = f.Close()
	if err != nil {
		log.Fatal("%s", err)
	}

	nShards := 1 + rand.Intn(24)
	nParity := 1 + rand.Intn(8)
	hashRate := int64(128 + (1 << uint(rand.Intn(24))))
	err = rdso.EncodeFile(f.Name(), f.Name()+".rs", nShards, nParity, hashRate)
	defer os.Remove(f.Name() + ".rs")
	if err != nil {
		log.Fatal("%s", err)
	}

	err = rdso.CheckFile(f.Name(), f.Name()+".rs", log)
	if err != nil || log.NErrors > 0 {
		os.Exit(1)
	}

	nErrors := rand.Intn(nParity)
	if nErrors < nParity {
		nErrors++
	}

	f, err = os.OpenFile(name, os.O_RDWR, 0644)
	if err != nil {
		log.Fatal("%s: %s", name, err)
	}
	for i := 0; i < nErrors; i++ {
		offset := rand.Int63n(int64(len))
		var b [1]byte
		_, err = f.ReadAt(b[:], offset)
		if err != nil {
			log.Fatal("%s", err)
		}

		delta := 1 + rand.Intn(254)
		b[0] = byte((int(b[0]) + delta) % 255)

		_, err = f.WriteAt(b[:], offset)
		if err != nil {
			log.Fatal("%s", err)
		}
	}
	f.Close()

	err = rdso.CheckFile(f.Name(), f.Name()+".rs", log)
	if err != nil {
		log.Fatal("%s", err)
	}
	if log.NErrors == 0 {
		log.Fatal("CheckFile of corrupted file didn't fail?")
	}
	log.NErrors = 0

	err = rdso.RestoreFile(f.Name(), f.Name()+".rs", log)
	//defer os.Remove(f.Name() + ".recovered")
	if log.NErrors > 0 {
		os.Exit(1)
	}

	err = rdso.CheckFile(f.Name()+".recovered", f.Name()+".rs", log)
	if err != nil {
		log.Fatal("%s", err)
	} else if log.NErrors > 0 {
		log.Fatal("check of recovered failed?")
	}
}

type countingWriter struct {
	io.Writer
	Count int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.Count++
	return w.Writer.Write(p)
}
