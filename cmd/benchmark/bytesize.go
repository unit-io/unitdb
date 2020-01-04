package main

import "fmt"

// Code from http://golang.org/doc/effective_go.html
type byteSize float64

const (
	_           = iota // ignore first value by assigning to blank identifier
	kb byteSize = 1 << (10 * iota)
	mb
	gb
	tb
	pb
	eb
	zb
	yb
)

func (b byteSize) String() string {
	switch {
	case b >= yb:
		return fmt.Sprintf("%.2fYB", b/yb)
	case b >= zb:
		return fmt.Sprintf("%.2fZB", b/zb)
	case b >= eb:
		return fmt.Sprintf("%.2fEB", b/eb)
	case b >= pb:
		return fmt.Sprintf("%.2fPB", b/pb)
	case b >= tb:
		return fmt.Sprintf("%.2fTB", b/tb)
	case b >= gb:
		return fmt.Sprintf("%.2fGB", b/gb)
	case b >= mb:
		return fmt.Sprintf("%.2fMB", b/mb)
	case b >= kb:
		return fmt.Sprintf("%.2fKB", b/kb)
	}
	return fmt.Sprintf("%.2fB", b)
}
