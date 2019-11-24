package hash

import "math/bits"

const (
	fleaSeed       = uint32(0xf1ea5eed)
	fleaRot1       = 27
	fleaRot2       = 17
	fleaInitRounds = 3 // initializing with 3 rounds works well enough in practice
)

// "A small noncryptographic PRNG" (Jenkins, 2007)
// * http://burtleburtle.net/bob/rand/smallprng.html
// * https://groups.google.com/d/msg/sci.crypt.random-numbers/LAuBGOErdrk/xrMBr3guA7IJ
//
// Also known as FLEA
func fleaInit(key uint64) (a, b, c, d uint32) {
	seed := uint32((key >> 32) ^ key)
	a, b, c, d = fleaSeed, seed, seed, seed
	i := 0
	// Functions containing for-loops cannot currently be inlined.
	// See https://github.com/golang/go/issues/14768
loop:
	e := a - bits.RotateLeft32(b, fleaRot1)
	a = b ^ bits.RotateLeft32(c, fleaRot2)
	b = c + d
	c = d + e
	d = e + a
	i++
	if i < fleaInitRounds {
		goto loop
	}
	return
}

func fleaRound(a, b, c, d uint32) (uint32, uint32, uint32, uint32) {
	e := a - bits.RotateLeft32(b, fleaRot1)
	a = b ^ bits.RotateLeft32(c, fleaRot2)
	b = c + d
	c = d + e
	d = e + a
	return a, b, c, d
}
