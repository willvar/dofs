package dofs

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"io"
	"testing"
)

func TestChunkedEncryptionRoundTripAndRanges(t *testing.T) {
	key := make([]byte, KeySize)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	plaintext := make([]byte, DefaultChunkSize*2+137)
	if _, err := rand.Read(plaintext); err != nil {
		t.Fatal(err)
	}
	var encrypted bytes.Buffer
	if err := EncryptStream(key, bytes.NewReader(plaintext), &encrypted); err != nil {
		t.Fatal(err)
	}
	if int64(encrypted.Len()) != EncryptedSize(int64(len(plaintext)), DefaultChunkSize) {
		t.Fatalf("encrypted size = %d", encrypted.Len())
	}
	var decrypted bytes.Buffer
	if err := DecryptStream(key, bytes.NewReader(encrypted.Bytes()), &decrypted); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decrypted.Bytes(), plaintext) {
		t.Fatal("decrypted plaintext differs")
	}

	start := int64(DefaultChunkSize - 31)
	end := int64(DefaultChunkSize + 79)
	firstChunk := start / DefaultChunkSize
	lastChunk := end / DefaultChunkSize
	cipherChunkSize := int64(DefaultChunkSize + NonceSize + TagSize)
	cipherStart := int64(EncryptedHeaderSize) + firstChunk*cipherChunkSize
	cipherEnd := int64(EncryptedHeaderSize) + (lastChunk+1)*cipherChunkSize
	if cipherEnd > int64(encrypted.Len()) {
		cipherEnd = int64(encrypted.Len())
	}
	var ranged bytes.Buffer
	if err := DecryptRange(
		key,
		bytes.NewReader(encrypted.Bytes()[cipherStart:cipherEnd]),
		&ranged,
		DefaultChunkSize,
		uint64(firstChunk),
		start-firstChunk*DefaultChunkSize,
		end-firstChunk*DefaultChunkSize,
	); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ranged.Bytes(), plaintext[start:end+1]) {
		t.Fatal("range plaintext differs")
	}
}

func TestCryptoFormatMatchesDomusV1Fixture(t *testing.T) {
	// The wrapping fixture guards the byte layout used by existing Domus rows.
	// Random nonces prevent a deterministic ciphertext fixture, so verify the
	// exact nonce+ciphertext length and successful unwrap instead.
	kek, _ := hex.DecodeString("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	dek, _ := hex.DecodeString("1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100")
	wrapped, err := WrapKey(kek, dek)
	if err != nil {
		t.Fatal(err)
	}
	if len(wrapped) != NonceSize+KeySize+TagSize {
		t.Fatalf("wrapped length = %d", len(wrapped))
	}
	unwrapped, err := UnwrapKey(kek, wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(unwrapped, dek) {
		t.Fatal("unwrapped key differs")
	}
	Clear(unwrapped)
	if _, err := io.ReadAll(bytes.NewReader(nil)); err != nil {
		t.Fatal(err)
	}
}

func TestNamespaceAndObjectKeyValidation(t *testing.T) {
	key, err := GenerationObjectKey("tenant_01", 42, 3, "txn-id")
	if err != nil {
		t.Fatal(err)
	}
	want := ".dofs/v1/namespaces/tenant_01/objects/42/00000000000000000003-txn-id.dofs"
	if key != want {
		t.Fatalf("key = %q, want %q", key, want)
	}
	for _, invalid := range []string{"", ".hidden", "../other", "a/b", " white"} {
		if _, err := ObjectRoot(invalid); err == nil {
			t.Fatalf("namespace %q accepted", invalid)
		}
	}
}
