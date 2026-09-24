package security

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base32"
	"encoding/binary"
	"errors"
)

// Signed keys carry an HMAC tag computed with a server secret over the key
// and the contract it was issued for. Without the tag, anyone could edit the
// permissions or the target of a key, or mint keys for a known contract.
const (
	tagLen       = 8 // 64 bit tag: a forgery needs about 2^63 requests to the server.
	signedRawLen = rawLen + tagLen

	// SignedKeyLen is the length of an encoded signed key.
	SignedKeyLen = 26
)

var keyEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// Signed key errors
var (
	ErrInvalidKey       = errors.New("Key provided is invalid")
	ErrInvalidSignature = errors.New("Key signature is invalid")
)

// Signer issues and verifies signed keys.
type Signer struct {
	key []byte
}

// NewSigner returns a Signer whose signing key is derived from secret, so the
// secret itself is never used directly as an HMAC key.
func NewSigner(secret []byte) *Signer {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte("unitdb topic key v1"))
	return &Signer{key: m.Sum(nil)}
}

func (s *Signer) tag(contract uint32, raw []byte) []byte {
	var c [4]byte
	binary.BigEndian.PutUint32(c[:], contract)
	m := hmac.New(sha256.New, s.key)
	m.Write(c[:])
	m.Write(raw)
	return m.Sum(nil)[:tagLen]
}

// GenerateKey generates a new key for topic, signed for contract.
func (s *Signer) GenerateKey(contract uint32, topic string, permissions uint32) (string, error) {
	key := Key(make([]byte, rawLen))
	key.SetPermissions(permissions)
	if err := key.SetTarget(contract, topic); err != nil {
		return "", err
	}
	buf := make([]byte, 0, signedRawLen)
	buf = append(buf, key...)
	buf = append(buf, s.tag(contract, key)...)
	return keyEncoding.EncodeToString(buf), nil
}

// DecodeKey decodes a signed key and checks that the server issued it for
// contract.
func (s *Signer) DecodeKey(contract uint32, text string) (Key, error) {
	if len(text) != SignedKeyLen {
		return nil, ErrInvalidKey
	}
	buf, err := keyEncoding.DecodeString(text)
	if err != nil || len(buf) != signedRawLen {
		return nil, ErrInvalidKey
	}
	raw, tag := buf[:rawLen], buf[rawLen:]
	if !hmac.Equal(tag, s.tag(contract, raw)) {
		return nil, ErrInvalidSignature
	}
	return Key(raw), nil
}
