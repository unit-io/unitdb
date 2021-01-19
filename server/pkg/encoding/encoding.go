package encoding

const (
	// encoding stores a custom version of the base32 encoding.
	encoding = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
)

var (
	// dec is the decoding map for base32 encoding
	dec [256]byte
)
