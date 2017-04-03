// storage/storage_test.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package storage

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

func TestSimple(t *testing.T) {
	for _, backend := range getStorage(t) {
		// Write something simple and get it back.
		simple := []byte{0, 1, 2, 3, 4, 5}
		hash := backend.Write(simple)

		if !backend.HashExists(hash) {
			t.Errorf("%s: hash doesn't exist even though just written?", backend)
		}

		backend.SyncWrites()

		r, err := backend.Read(hash)
		if err != nil {
			t.Errorf("%s: read: %v", backend, err)
		}
		b, err := ioutil.ReadAll(r)
		if err != nil {
			t.Errorf("%s: read all: %v", backend, err)
		}
		if bytes.Compare(simple, b) != 0 {
			t.Errorf("%s: bytes mismatch: wrote %+v, read %+v", backend, simple, b)
		}
	}
}

func TestNamed(t *testing.T) {
	for _, backend := range getStorage(t) {
		// Now another simple thing, but named.
		namedsimple := []byte{5, 100, 2, 0, 1, 2, 3, 4, 5}
		hash := backend.Write(namedsimple)
		backend.WriteMetadata("foo", hash[:])
		backend.SyncWrites()

		if !backend.MetadataExists("foo") {
			t.Errorf("%s: named object \"foo\" not found", backend)
		} else {
			b := backend.ReadMetadata("foo")
			var h Hash
			copy(h[:], b)

			r, err := backend.Read(h)
			if err != nil {
				t.Errorf("%s: read: %v", backend, err)
			}
			b, err = ioutil.ReadAll(r)
			if err != nil {
				t.Errorf("%s: read all: %v", backend, err)
			}
			if bytes.Compare(namedsimple, b) != 0 {
				t.Errorf("%s: bytes mismatch: wrote %+v, read %+v",
					backend, namedsimple, b)
			}
		}
	}
}

func TestMetadata(t *testing.T) {
	for _, backend := range getStorage(t) {
		backend.WriteMetadata("blurp", []byte("hello"))
		backend.WriteMetadata("flurg", []byte("world"))
		if !backend.MetadataExists("blurp") {
			t.Errorf("%s: missing metadata", backend)
		}
		if !backend.MetadataExists("flurg") {
			t.Errorf("%s: missing metadata", backend)
		}
		if backend.MetadataExists("flurgz") {
			t.Errorf("%s: unexpected metadata", backend)
		}
		if string(backend.ReadMetadata("blurp")) != "hello" {
			t.Errorf("%s: unexpected metadata value", backend)
		}
		if string(backend.ReadMetadata("flurg")) != "world" {
			t.Errorf("%s: unexpected metadata value", backend)
		}
	}
}

func TestMany(t *testing.T) {
	for _, backend := range getStorage(t) {
		// Write 200 items, where the i'th item is i bytes long, all having
		// value i.
		var hashes []Hash
		var written [][]byte
		for i := 1; i < 200; i++ {
			b := make([]byte, i)
			for j := 0; j < i; j++ {
				b[j] = byte(i)
			}
			hashes = append(hashes, backend.Write(b))
			written = append(written, b)

			if rand.Intn(10) == 0 {
				backend.SyncWrites()
			}
		}
		backend.SyncWrites()

		// Read them back individually, one at a time
		for i, hash := range hashes {
			r, err := backend.Read(hash)
			if err != nil {
				t.Errorf("%s: read: %v", backend, err)
			}
			chunk, err := ioutil.ReadAll(r)
			if err != nil {
				t.Errorf("%s: readall: %v", backend, err)
			}
			r.Close()

			if len(chunk) != len(written[i]) {
				t.Errorf("%s: got length %d, expected %d", backend,
					len(chunk), len(written[i]))
			}
			if bytes.Compare(chunk, written[i]) != 0 {
				t.Errorf("%s: didn't get same bytes back. hash %s, wrote %+v, got %+v",
					backend, hash, written[i], chunk)
			}
		}
	}
}

func genRandom(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

func TestManyRandom(t *testing.T) {
	for _, backend := range getStorage(t) {
		var hashes []Hash
		var chunks [][]byte
		const count = 5000

		for i := 0; i < count; i++ {
			buf := genRandom(rand.Intn(32 * 1024))
			hash := backend.Write(buf)
			hashes = append(hashes, hash)
			chunks = append(chunks, buf)

			if rand.Int31n(5) == 0 {
				backend.SyncWrites()
			}
		}
		backend.SyncWrites()

		perm := rand.Perm(count)
		for _, i := range perm {
			// Read the saved bytes from backend.
			r, err := backend.Read(hashes[i])
			if err != nil {
				t.Fatalf("%s: %d: %v", backend, i, err)
			}

			c, err := ioutil.ReadAll(r)
			r.Close()
			if err != nil {
				t.Fatalf("%s: %d: %v", backend, i, err)
			}

			// Make sure the two match
			if bytes.Compare(c, chunks[i]) != 0 {
				t.Errorf("%s: %d: didn't get same bytes back!", backend, i)
			}
		}
	}
}

func getStorage(t *testing.T) []Backend {
	var b []Backend

	b = append(b, NewMemory())
	b = append(b, NewCompressed(NewMemory()))
	b = append(b, NewEncrypted(NewMemory(), "foobar"))

	i := 0
	getDir := func() string {
		path := fmt.Sprintf("/tmp/bk_storage_test-%d", i)
		i++

		if _, err := os.Stat(path); err == nil {
			err := os.RemoveAll(path)
			if err != nil {
				t.Errorf("remove all: %v", err)
			}
		}
		err := os.Mkdir(path, 0700)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		return path
	}

	b = append(b, NewDisk(getDir()))
	b = append(b, NewEncrypted(NewDisk(getDir()), "foobar"))
	b = append(b, NewCompressed(NewEncrypted(NewDisk(getDir()), "foobar")))

	return b
}
