package encoding

func init() {
	for i := 0; i < len(dec); i++ {
		dec[i] = 0xFF
	}
	for i := 0; i < len(encoding); i++ {
		dec[encoding[i]] = byte(i)
	}
}

// encode by unrolling the stdlib base32 algorithm + removing all safe checks
func Encode8(dst, id []byte) {
	dst[0] = encoding[id[0]>>3]
	dst[1] = encoding[(id[1]>>6)&0x1F|(id[0]<<2)&0x1F]
	dst[2] = encoding[(id[1]>>1)&0x1F]
	dst[3] = encoding[(id[2]>>4)&0x1F|(id[1]<<4)&0x1F]
	dst[4] = encoding[id[3]>>7|(id[2]<<1)&0x1F]
	dst[5] = encoding[(id[3]>>2)&0x1F]
	dst[6] = encoding[id[4]>>5|(id[3]<<3)&0x1F]
	dst[7] = encoding[id[4]&0x1F]
	dst[8] = encoding[id[5]>>3]
	dst[9] = encoding[(id[6]>>6)&0x1F|(id[5]<<2)&0x1F]
	dst[10] = encoding[(id[6]>>1)&0x1F]
	dst[11] = encoding[(id[7]>>4)&0x1F|(id[6]<<4)&0x1F]
	dst[12] = encoding[(id[7]<<1)&0x1F]
}

// decode by unrolling the stdlib base32 algorithm + removing all safe checks
func Decode8(id []byte, src []byte) {
	id[0] = dec[src[0]]<<3 | dec[src[1]]>>2
	id[1] = dec[src[1]]<<6 | dec[src[2]]<<1 | dec[src[3]]>>4
	id[2] = dec[src[3]]<<4 | dec[src[4]]>>1
	id[3] = dec[src[4]]<<7 | dec[src[5]]<<2 | dec[src[6]]>>3
	id[4] = dec[src[6]]<<5 | dec[src[7]]
	id[5] = dec[src[8]]<<3 | dec[src[9]]>>2
	id[6] = dec[src[9]]<<6 | dec[src[10]]<<1 | dec[src[11]]>>4
	id[7] = dec[src[11]]<<4 | dec[src[12]]>>1
}
