package memory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/willvar/dofs"
)

type Store struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func New() *Store { return &Store{objects: make(map[string][]byte)} }

func validateKey(key string) error {
	if key == "" || strings.HasPrefix(key, "/") || strings.Contains(key, "\x00") {
		return errors.New("invalid object key")
	}
	return nil
}

func (s *Store) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateKey(key); err != nil || start < 0 || end < start {
		return nil, errors.New("invalid object range")
	}
	s.mu.RLock()
	value, exists := s.objects[key]
	if exists {
		value = append([]byte(nil), value...)
	}
	s.mu.RUnlock()
	if !exists {
		return nil, dofs.ErrObjectNotFound
	}
	if start >= int64(len(value)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if end >= int64(len(value)) {
		end = int64(len(value)) - 1
	}
	return io.NopCloser(bytes.NewReader(value[start : end+1])), nil
}

func (s *Store) Stat(ctx context.Context, key string) (dofs.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return dofs.ObjectInfo{}, err
	}
	if err := validateKey(key); err != nil {
		return dofs.ObjectInfo{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, exists := s.objects[key]
	if !exists {
		return dofs.ObjectInfo{}, dofs.ErrObjectNotFound
	}
	return dofs.ObjectInfo{Size: int64(len(value))}, nil
}

func (s *Store) Put(ctx context.Context, key string, source io.Reader, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateKey(key); err != nil || size < 0 {
		return errors.New("invalid object")
	}
	value, err := io.ReadAll(io.LimitReader(source, size+1))
	if err != nil {
		return err
	}
	if int64(len(value)) != size {
		return fmt.Errorf("object size = %d, want %d", len(value), size)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.objects[key]; exists {
		return dofs.ErrAlreadyExists
	}
	s.objects[key] = value
	return nil
}

func (s *Store) Copy(ctx context.Context, sourceKey, destinationKey string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateKey(sourceKey); err != nil {
		return err
	}
	if err := validateKey(destinationKey); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, exists := s.objects[sourceKey]
	if !exists {
		return dofs.ErrObjectNotFound
	}
	if _, exists := s.objects[destinationKey]; exists {
		return dofs.ErrAlreadyExists
	}
	s.objects[destinationKey] = append([]byte(nil), value...)
	return nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateKey(key); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.objects[key]; !exists {
		return dofs.ErrObjectNotFound
	}
	delete(s.objects, key)
	return nil
}

func (s *Store) DeletePrefix(ctx context.Context, prefix string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateKey(prefix); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			delete(s.objects, key)
		}
	}
	return nil
}

func (s *Store) Bytes(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, exists := s.objects[key]
	return append([]byte(nil), value...), exists
}

var _ dofs.ObjectStore = (*Store)(nil)
