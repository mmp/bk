// storage/memory.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

package storage

import (
	"bytes"
	"io"
	"io/ioutil"
	"time"
)

type metadata struct {
	data    []byte
	created time.Time
}

type memory struct {
	blobs map[Hash][]byte
	meta  map[string]metadata
}

// Duplicate the provided byte slice.
func dupe(src []byte) []byte {
	d := make([]byte, len(src))
	copy(d, src)
	return d
}

// NewMemory returns a storage.Backend that stores all data--blobs, hashes,
// metadata, etc., in RAM.  It's really only useful for testing of code
// built on top of storage.Backend, where we may want to save the trouble
// of saving a bunch of stuff to disk.
func NewMemory() Backend {
	return &memory{
		blobs: make(map[Hash][]byte),
		meta:  make(map[string]metadata),
	}
}

func (m *memory) String() string {
	return "memory"
}

func (m *memory) LogStats() {
}

func (m *memory) Fsck() {
}

func (m *memory) Write(data []byte) Hash {
	hash := HashBytes(data)
	// Blobs are stored in a map; only add the data if it isn't already
	// there.
	if _, ok := m.blobs[hash]; !ok {
		m.blobs[hash] = dupe(data)
	}
	return hash
}

func (m *memory) HashExists(hash Hash) bool {
	_, ok := m.blobs[hash]
	return ok
}

func (m *memory) Hashes() map[Hash]struct{} {
	ret := make(map[Hash]struct{})
	for h := range m.blobs {
		var empty struct{}
		ret[h] = empty
	}
	return ret
}

func (m *memory) SyncWrites() {
}

func (m *memory) Read(hash Hash) (io.ReadCloser, error) {
	if b, ok := m.blobs[hash]; !ok {
		return nil, ErrHashNotFound
	} else {
		return ioutil.NopCloser(bytes.NewReader(b)), nil
	}
}

func (m *memory) WriteMetadata(name string, data []byte) {
	if _, ok := m.meta[name]; ok {
		log.Fatal("metadata already exists")
	}
	m.meta[name] = metadata{dupe(data), time.Now()}
}

func (m *memory) ReadMetadata(name string) []byte {
	md, ok := m.meta[name]
	if !ok {
		log.Fatal("metadata not found")
	}
	return dupe(md.data)
}

func (m *memory) MetadataExists(name string) bool {
	_, ok := m.meta[name]
	return ok
}

func (m *memory) ListMetadata() map[string]time.Time {
	md := make(map[string]time.Time)
	for name, meta := range m.meta {
		md[name] = meta.created
	}
	return md
}
