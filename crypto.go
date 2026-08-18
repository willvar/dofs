package dofs

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

const (
	CryptoVersion       byte = 0x01
	DefaultChunkSize         = 64 * 1024
	NonceSize                = 12
	TagSize                  = 16
	KeySize                  = 32
	EncryptedHeaderSize      = 5
)

func GenerateKey() ([]byte, error) {
	key := make([]byte, KeySize)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate encryption key: %w", err)
	}
	return key, nil
}

func WrapKey(kek, plaintextKey []byte) ([]byte, error) {
	gcm, err := newGCM(kek)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate key nonce: %w", err)
	}
	return gcm.Seal(nonce, nonce, plaintextKey, nil), nil
}

func UnwrapKey(kek, wrapped []byte) ([]byte, error) {
	gcm, err := newGCM(kek)
	if err != nil {
		return nil, err
	}
	if len(wrapped) < gcm.NonceSize()+gcm.Overhead() {
		return nil, errors.New("wrapped key is too short")
	}
	nonce := wrapped[:gcm.NonceSize()]
	plaintext, err := gcm.Open(nil, nonce, wrapped[gcm.NonceSize():], nil)
	if err != nil {
		return nil, fmt.Errorf("unwrap key: %w", err)
	}
	return plaintext, nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("encryption key must be %d bytes", KeySize)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// EncryptStream writes the stable DOFS v1 chunked AES-256-GCM format. Each
// chunk authenticates its ordinal as AAD, preventing reorder attacks.
func EncryptStream(key []byte, source io.Reader, destination io.Writer) error {
	gcm, err := newGCM(key)
	if err != nil {
		return err
	}
	header := [EncryptedHeaderSize]byte{CryptoVersion}
	binary.BigEndian.PutUint32(header[1:], uint32(DefaultChunkSize))
	if _, err := destination.Write(header[:]); err != nil {
		return err
	}

	buffer := make([]byte, DefaultChunkSize)
	nonce := make([]byte, gcm.NonceSize())
	var chunk uint64
	for {
		read, readErr := io.ReadFull(source, buffer)
		if read > 0 {
			if _, err := rand.Read(nonce); err != nil {
				return fmt.Errorf("generate chunk nonce: %w", err)
			}
			aad := make([]byte, 8)
			binary.BigEndian.PutUint64(aad, chunk)
			if _, err := destination.Write(nonce); err != nil {
				return err
			}
			if _, err := destination.Write(gcm.Seal(nil, nonce, buffer[:read], aad)); err != nil {
				return err
			}
			chunk++
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func DecryptStream(key []byte, source io.Reader, destination io.Writer) error {
	chunkSize, err := ReadHeader(source)
	if err != nil {
		return err
	}
	gcm, err := newGCM(key)
	if err != nil {
		return err
	}
	buffer := make([]byte, NonceSize+chunkSize+TagSize)
	var chunk uint64
	for {
		if _, err := io.ReadFull(source, buffer[:NonceSize]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		read, readErr := io.ReadFull(source, buffer[NonceSize:])
		if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			return readErr
		}
		if read == 0 {
			return nil
		}
		aad := make([]byte, 8)
		binary.BigEndian.PutUint64(aad, chunk)
		plaintext, err := gcm.Open(nil, buffer[:NonceSize], buffer[NonceSize:NonceSize+read], aad)
		if err != nil {
			return fmt.Errorf("decrypt chunk %d: %w", chunk, err)
		}
		if _, err := destination.Write(plaintext); err != nil {
			return err
		}
		chunk++
		if errors.Is(readErr, io.ErrUnexpectedEOF) {
			return nil
		}
	}
}

func ReadHeader(source io.Reader) (int, error) {
	var header [EncryptedHeaderSize]byte
	if _, err := io.ReadFull(source, header[:]); err != nil {
		return 0, fmt.Errorf("read encrypted header: %w", err)
	}
	if header[0] != CryptoVersion {
		return 0, fmt.Errorf("unsupported crypto version %d", header[0])
	}
	chunkSize := int(binary.BigEndian.Uint32(header[1:]))
	if chunkSize <= 0 || chunkSize > 16*1024*1024 {
		return 0, fmt.Errorf("invalid encrypted chunk size %d", chunkSize)
	}
	return chunkSize, nil
}

// DecryptRange decrypts raw encrypted chunks without the five-byte header.
// trimStart and trimEnd are inclusive plaintext offsets relative to startChunk.
func DecryptRange(key []byte, source io.Reader, destination io.Writer, chunkSize int, startChunk uint64, trimStart, trimEnd int64) error {
	if chunkSize <= 0 || trimStart < 0 || trimEnd < trimStart {
		return errors.New("invalid plaintext range")
	}
	gcm, err := newGCM(key)
	if err != nil {
		return err
	}
	buffer := make([]byte, NonceSize+chunkSize+TagSize)
	chunk := startChunk
	var plaintextPosition int64
	var written int64
	target := trimEnd - trimStart + 1
	for written < target {
		if _, err := io.ReadFull(source, buffer[:NonceSize]); err != nil {
			return err
		}
		read, readErr := io.ReadFull(source, buffer[NonceSize:])
		if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			return readErr
		}
		if read == 0 {
			break
		}
		aad := make([]byte, 8)
		binary.BigEndian.PutUint64(aad, chunk)
		plaintext, err := gcm.Open(nil, buffer[:NonceSize], buffer[NonceSize:NonceSize+read], aad)
		if err != nil {
			return fmt.Errorf("decrypt chunk %d: %w", chunk, err)
		}
		chunkEnd := plaintextPosition + int64(len(plaintext))
		start := int64(0)
		if trimStart > plaintextPosition {
			start = trimStart - plaintextPosition
		}
		end := int64(len(plaintext))
		if trimEnd+1 < chunkEnd {
			end = trimEnd + 1 - plaintextPosition
		}
		if start < end {
			if _, err := destination.Write(plaintext[start:end]); err != nil {
				return err
			}
			written += end - start
		}
		plaintextPosition = chunkEnd
		chunk++
		if errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
	}
	if written != target {
		return fmt.Errorf("short plaintext range: got %d bytes, want %d", written, target)
	}
	return nil
}

func EncryptedSize(plaintextSize int64, chunkSize int64) int64 {
	if plaintextSize <= 0 {
		return EncryptedHeaderSize
	}
	chunks := (plaintextSize + chunkSize - 1) / chunkSize
	return EncryptedHeaderSize + plaintextSize + chunks*(NonceSize+TagSize)
}

func CheckedEncryptedSize(plaintextSize int64, chunkSize int64) (int64, error) {
	if plaintextSize < 0 || chunkSize <= 0 {
		return 0, errors.New("invalid encrypted object geometry")
	}
	if plaintextSize == 0 {
		return EncryptedHeaderSize, nil
	}
	if plaintextSize > math.MaxInt64-(chunkSize-1) {
		return 0, errors.New("encrypted object size overflow")
	}
	chunks := (plaintextSize + chunkSize - 1) / chunkSize
	overhead := int64(NonceSize + TagSize)
	if chunks > (math.MaxInt64-EncryptedHeaderSize-plaintextSize)/overhead {
		return 0, errors.New("encrypted object size overflow")
	}
	return EncryptedHeaderSize + plaintextSize + chunks*overhead, nil
}

func Clear(data []byte) {
	for index := range data {
		data[index] = 0
	}
}
