package dofs

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// EnvelopeKeyProvider keeps one deployment master key in memory and unwraps
// per-namespace KEKs stored in Namespace.WrappedKEK. The provider owns its copy
// of the master key; Close clears that copy.
type EnvelopeKeyProvider struct {
	mu     sync.RWMutex
	master []byte
}

func NewEnvelopeKeyProvider(masterKey []byte) (*EnvelopeKeyProvider, error) {
	if len(masterKey) != KeySize {
		return nil, fmt.Errorf("master key must be %d bytes", KeySize)
	}
	return &EnvelopeKeyProvider{master: append([]byte(nil), masterKey...)}, nil
}

func (p *EnvelopeKeyProvider) UnwrapNamespaceKey(_ context.Context, namespace Namespace) ([]byte, error) {
	if p == nil {
		return nil, errors.New("envelope key provider is nil")
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.master) != KeySize {
		return nil, errors.New("envelope key provider is closed")
	}
	key, err := UnwrapKey(p.master, namespace.WrappedKEK)
	if err != nil {
		return nil, fmt.Errorf("unwrap namespace %s key: %w", namespace.ID, err)
	}
	if len(key) != KeySize {
		Clear(key)
		return nil, fmt.Errorf("namespace %s key has invalid length", namespace.ID)
	}
	return key, nil
}

// WrapNewNamespaceKey generates a fresh namespace KEK and wraps it for
// persistence. Only the wrapped value is returned.
func (p *EnvelopeKeyProvider) WrapNewNamespaceKey() ([]byte, error) {
	if p == nil {
		return nil, errors.New("envelope key provider is nil")
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.master) != KeySize {
		return nil, errors.New("envelope key provider is closed")
	}
	key, err := GenerateKey()
	if err != nil {
		return nil, err
	}
	defer Clear(key)
	wrapped, err := WrapKey(p.master, key)
	if err != nil {
		return nil, fmt.Errorf("wrap namespace key: %w", err)
	}
	return wrapped, nil
}

func (p *EnvelopeKeyProvider) Close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	Clear(p.master)
	p.master = nil
}
