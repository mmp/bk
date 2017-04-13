// storage/encrypted.go
// Copyright(c) 2017 Matt Pharr
// BSD licensed; see LICENSE for details.

// Portions derived from skicka, (c) 2016 Google, Inc. (BSD licensed).

package storage

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"golang.org/x/crypto/pbkdf2"
	"io"
	"io/ioutil"
	"strings"
	"time"
)

type encpair struct {
	Plain, Encrypted Hash
}

// encrypted implements the storage.Backend interface. It encrypts /
// decrypts chunk data as it passes through the Read() and Write() methods.
type encrypted struct {
	backend Backend
	key     []byte
	// toEncrypted is a map from hashes of unencrypted chunks to hashes of
	// encrypted versions of them, if we already have them stored.  Because
	// we use a unique random new IV every time a new chunk comes in to
	// Write(), we need to maintain this map explicitly in for
	// deduplication to work.
	toEncrypted map[Hash]Hash
	// toEncryptedLog stores a log of the mappings added during the current
	// run; it's serialized to disk in SyncWrites().
	toEncryptedLog []encpair
}

type encryptedKey struct {
	salt           []byte
	passphraseHash []byte
	encryptedKey   []byte
	encryptedKeyIV []byte
}

const toEncryptedPrefix = "toencrypted-"

const ivLength = aes.BlockSize

// NewEncrypted returns a storage.Backend that applies AES encryption
// to the chunk data stored in the underlying storage.Backend.
// Note: metadata contents and the names of named hashes are not encrypted.
func NewEncrypted(backend Backend, passphrase string) Backend {
	eb := &encrypted{backend: backend,
		toEncrypted: make(map[Hash]Hash)}

	if backend.MetadataExists("encrypt.txt") {
		eb.key = getEncryptionKey(string(backend.ReadMetadata("encrypt.txt")),
			passphrase)
	} else {
		// Generate all of the values we need for encryption.
		var ec encryptedKey
		eb.key, ec = generateKey(passphrase)

		// And store them, hex-encoded, as metadata in the underlying backend.
		enc := fmt.Sprintf("%s\n", hex.EncodeToString(ec.salt))
		enc += fmt.Sprintf("%s\n", hex.EncodeToString(ec.passphraseHash))
		enc += fmt.Sprintf("%s\n", hex.EncodeToString(ec.encryptedKey))
		enc += fmt.Sprintf("%s\n", hex.EncodeToString(ec.encryptedKeyIV))
		eb.backend.WriteMetadata("encrypt.txt", []byte(enc))
	}

	// Process the contents of all of the log files that store pairs of
	// (plaintext, encrypted) hashes to populate the toEncryted map.
	for name := range backend.ListMetadata() {
		if !strings.HasPrefix(name, toEncryptedPrefix) {
			continue
		}

		md := backend.ReadMetadata(name)
		mh := DecodeMerkleHash(bytes.NewReader(md))

		r := mh.NewReader(nil, eb)
		dec := gob.NewDecoder(r)
		var toEncryptedLog []encpair
		log.CheckError(dec.Decode(&toEncryptedLog))

		for _, v := range toEncryptedLog {
			eb.toEncrypted[v.Plain] = v.Encrypted
		}
		log.CheckError(r.Close())
	}

	return eb
}

func (eb *encrypted) String() string {
	return "encrypted " + eb.backend.String()
}

func (eb *encrypted) LogStats() {
	eb.backend.LogStats()
}

func (eb *encrypted) Fsck() {
	// TODO? Validate the plaintext->encrypted hashes?  It's probably fine
	// to assume that Reed-Solomon suffices for any integrtity issues for
	// those..
	eb.backend.Fsck()
}

func (eb *encrypted) Write(data []byte) Hash {
	// See if we've already stored these bytes; return the hash of
	// their encrypted version if so.
	hplain := HashBytes(data)
	if henc, ok := eb.toEncrypted[hplain]; ok {
		return henc
	}

	// Generate a new random initialization vector and encrypt the data.
	iv := getRandomBytes(ivLength)
	enc := encryptBytes(eb.key, iv, data)
	// In the chunk that's stored, first write out the IV, then the
	// encrypted data.
	henc := eb.backend.Write(append(iv, enc...))

	// Update the map and the log so that if we see these bytes again, we
	// don't store them redundantly in the current and future runs,
	// respectively.
	eb.toEncrypted[hplain] = henc
	eb.toEncryptedLog = append(eb.toEncryptedLog, encpair{hplain, henc})

	return henc
}

func (eb *encrypted) SyncWrites() {
	// Make sure all of the chunks are stored.
	eb.backend.SyncWrites()

	// Store the log of any new mappings from unencrypted -> encrypted
	// hashes.
	if len(eb.toEncryptedLog) > 0 {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		log.CheckError(enc.Encode(eb.toEncryptedLog))

		// Important: use eb, not eb.backend, so these are encrypted!
		hash := MerkleFromSingle(eb.Write(buf.Bytes()))

		// The name doesn't matter but does need to be unique.
		name := toEncryptedPrefix + hash.Hash.String()
		eb.backend.WriteMetadata(name, hash.Bytes())

		// Now have the backend do its thing and make sure that the metadata
		// has also landed.
		eb.backend.SyncWrites()

		eb.toEncryptedLog = nil
	}
}

func (eb *encrypted) HashExists(hash Hash) bool {
	return eb.backend.HashExists(hash)
}

func (eb *encrypted) Hashes() map[Hash]struct{} {
	return eb.backend.Hashes()
}

func (eb *encrypted) Read(hash Hash) (io.ReadCloser, error) {
	r, err := eb.backend.Read(hash)
	if err != nil {
		return r, err
	}
	// First read the initialization vector, which we stored at the
	// start of the stored chunk.
	var iv [ivLength]byte
	_, err = io.ReadFull(r, iv[:])
	if err != nil {
		return r, err
	}
	// With that, we can make a reader that will decrypt the rest of it.
	return &readerAndCloser{makeDecryptingReader(eb.key, iv[:], r), r}, nil
}

type readerAndCloser struct {
	io.Reader
	io.Closer
}

func (eb *encrypted) WriteMetadata(name string, data []byte) {
	eb.backend.WriteMetadata(name, data)
}

func (eb *encrypted) ReadMetadata(name string) []byte {
	return eb.backend.ReadMetadata(name)
}

func (eb *encrypted) MetadataExists(name string) bool {
	return eb.backend.MetadataExists(name)
}

func (eb *encrypted) ListMetadata() map[string]time.Time {
	return eb.backend.ListMetadata()
}

///////////////////////////////////////////////////////////////////////////

// Utility function to decode hex-encoded bytes; treats any encoding errors
// as fatal errors.
func decodeHexString(s string) []byte {
	r, err := hex.DecodeString(s)
	log.CheckError(err)
	return r
}

// Encrypt the given plaintext using the given encryption key 'key' and
// initialization vector 'iv'. The initialization vector should be 16 bytes
// (the AES block-size), and should be randomly generated and unique for
// each chunk of data that's encrypted.
func encryptBytes(key []byte, iv []byte, plaintext []byte) []byte {
	r, err := ioutil.ReadAll(makeEncryptingReader(key, iv,
		bytes.NewReader(plaintext)))
	log.CheckError(err)
	return r
}

// Returns an io.Reader that encrypts the byte stream from the given io.Reader
// using the given key and initialization vector.
func makeEncryptingReader(key []byte, iv []byte, reader io.Reader) io.Reader {
	block, err := aes.NewCipher(key)
	log.CheckError(err)
	log.Check(len(iv) == ivLength)
	stream := cipher.NewCFBEncrypter(block, iv[:])
	return &cipher.StreamReader{S: stream, R: reader}
}

// Decrypt the given cyphertext using the given encryption key and
// initialization vector 'iv'.
func decryptBytes(key []byte, iv []byte, ciphertext []byte) []byte {
	r, err := ioutil.ReadAll(makeDecryptingReader(key, iv,
		bytes.NewReader(ciphertext)))
	log.CheckError(err)
	return r
}

func makeDecryptingReader(key []byte, iv []byte, reader io.Reader) io.Reader {
	block, err := aes.NewCipher(key)
	log.CheckError(err)
	log.Check(len(iv) == ivLength)
	stream := cipher.NewCFBDecrypter(block, iv)
	return &cipher.StreamReader{S: stream, R: reader}
}

///////////////////////////////////////////////////////////////////////////
// Key generation, representation, and management.

// Return the given number of bytes of random values, using a
// cryptographically-strong random number source.
func getRandomBytes(n int) []byte {
	bytes := make([]byte, n)
	_, err := io.ReadFull(rand.Reader, bytes)
	log.CheckError(err)
	return bytes
}

// Create a new encryption key and encrypt it using the user-provided
// passphrase.
func generateKey(passphrase string) ([]byte, encryptedKey) {
	// Derive a 64-byte hash from the passphrase using PBKDF2 with 65536
	// rounds of SHA256.
	salt := getRandomBytes(32)
	hash := pbkdf2.Key([]byte(passphrase), salt, 65536, 64, sha256.New)
	log.Check(len(hash) == 64)

	// We'll store the first 32 bytes of the hash to use to confirm the
	// correct passphrase is given on subsequent runs.
	passHash := hash[:32]
	// And we'll use the remaining 32 bytes as a key to encrypt the actual
	// encryption key. (These bytes are *not* stored).
	keyEncryptKey := hash[32:]

	// Generate a random encryption key and encrypt it using the key
	// derived from the passphrase.
	key := getRandomBytes(32)
	iv := getRandomBytes(ivLength)

	return key, encryptedKey{
		salt:           salt,
		passphraseHash: passHash,
		encryptedKey:   encryptBytes(keyEncryptKey, iv, key),
		encryptedKeyIV: iv,
	}
}

func getEncryptionKey(enc string, passphrase string) []byte {
	// Parse the various values from the encryption config file text.
	var saltHex, passphraseHashHex, encKeyHex, encryptedKeyIVHex string
	n, err := fmt.Sscanf(enc, "%s\n%s\n%s\n%s", &saltHex, &passphraseHashHex,
		&encKeyHex, &encryptedKeyIVHex)
	log.CheckError(err)
	log.Check(n == 4)

	// Run the salted passphrase through PBKDF2 to (slowly) generate a
	// 64-byte derived key.
	salt := decodeHexString(saltHex)
	derivedKey := pbkdf2.Key([]byte(passphrase), salt, 65536, 64, sha256.New)

	// Make sure the first 32 bytes of the derived key match the bytes stored
	// when we first generated the key; if they don't, the user gave us
	// the wrong passphrase.
	passphraseHash := decodeHexString(passphraseHashHex)
	if !bytes.Equal(derivedKey[:32], passphraseHash) {
		log.Fatal("incorrect passphrase")
	}

	// Use the last 32 bytes of the derived key to decrypt the actual
	// encryption key.
	keyEncryptKey := derivedKey[32:]
	encryptedKeyIV := decodeHexString(encryptedKeyIVHex)
	encryptedKey := decodeHexString(encKeyHex)
	return decryptBytes(keyEncryptKey, encryptedKeyIV, encryptedKey)
}
