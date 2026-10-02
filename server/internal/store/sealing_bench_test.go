package store

import (
	"crypto/cipher"
	"fmt"
	"testing"

	"golang.org/x/crypto/chacha20poly1305"
)

// BenchmarkSealRecord measures what sealing adds to storing a record, and
// opening to reading one, by record size.
func BenchmarkSealRecord(b *testing.B) {
	aead, err := chacha20poly1305.NewX(make([]byte, chacha20poly1305.KeySize))
	if err != nil {
		b.Fatal(err)
	}
	s := &sealer{issue: 0, aeads: map[uint8]cipher.AEAD{0: aead}, seal: true}
	for _, size := range []int{64, 1024, 16384} {
		rec := make([]byte, size)
		ad := contractAD(1)
		b.Run(fmt.Sprintf("seal/%d", size), func(b *testing.B) {
			b.SetBytes(int64(size))
			for i := 0; i < b.N; i++ {
				if _, err := s.sealRecord(rec, ad); err != nil {
					b.Fatal(err)
				}
			}
		})
		sealed, _ := s.sealRecord(rec, ad)
		b.Run(fmt.Sprintf("open/%d", size), func(b *testing.B) {
			b.SetBytes(int64(size))
			for i := 0; i < b.N; i++ {
				if _, err := s.openRecord(sealed, ad); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
