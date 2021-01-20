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
func Encode32(dst, id []byte) {
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
	dst[12] = encoding[id[8]>>7|(id[7]<<1)&0x1F]
	dst[13] = encoding[(id[8]>>2)&0x1F]
	dst[14] = encoding[(id[9]>>5)|(id[8]<<3)&0x1F]
	dst[15] = encoding[id[9]&0x1F]
	dst[16] = encoding[id[10]>>3]
	dst[17] = encoding[(id[11]>>6)&0x1F|(id[10]<<2)&0x1F]
	dst[18] = encoding[(id[11]>>1)&0x1F]
	dst[19] = encoding[(id[12]>>4)&0x1F|(id[11]<<4)&0x1F]
	dst[20] = encoding[id[13]>>7|(id[12]<<1)&0x1F]
	dst[21] = encoding[(id[13]>>2)&0x1F]
	dst[22] = encoding[(id[14]>>5)|(id[13]<<3)&0x1F]
	dst[23] = encoding[id[14]&0x1F]
	dst[24] = encoding[id[15]>>3]
	dst[25] = encoding[(id[16]>>6)&0x1F|(id[15]<<2)&0x1F]
	dst[26] = encoding[(id[16]>>1)&0x1F]
	dst[27] = encoding[(id[17]>>4)&0x1F|(id[16]<<4)&0x1F]
	dst[28] = encoding[id[18]>>7|(id[17]<<1)&0x1F]
	dst[29] = encoding[(id[18]>>2)&0x1F]
	dst[30] = encoding[(id[19]>>5)|(id[18]<<3)&0x1F]
	dst[31] = encoding[id[19]&0x1F]
	dst[32] = encoding[id[20]>>3]
	dst[33] = encoding[(id[21]>>6)&0x1F|(id[20]<<2)&0x1F]
	dst[34] = encoding[(id[21]>>1)&0x1F]
	dst[35] = encoding[(id[22]>>4)&0x1F|(id[21]<<4)&0x1F]
	dst[36] = encoding[id[23]>>7|(id[22]<<1)&0x1F]
	dst[37] = encoding[(id[23]>>2)&0x1F]
	dst[38] = encoding[(id[24]>>5)|(id[23]<<3)&0x1F]
	dst[39] = encoding[id[24]&0x1F]
	dst[40] = encoding[id[25]>>3]
	dst[41] = encoding[(id[26]>>6)&0x1F|(id[25]<<2)&0x1F]
	dst[42] = encoding[(id[26]>>1)&0x1F]
	dst[43] = encoding[(id[27]>>4)&0x1F|(id[26]<<4)&0x1F]
	dst[44] = encoding[id[28]>>7|(id[27]<<1)&0x1F]
	dst[45] = encoding[(id[28]>>2)&0x1F]
	dst[46] = encoding[(id[29]>>5)|(id[28]<<3)&0x1F]
	dst[47] = encoding[id[29]&0x1F]
	dst[48] = encoding[id[30]>>3]
	dst[49] = encoding[(id[31]>>6)&0x1F|(id[30]<<2)&0x1F]
	dst[50] = encoding[(id[31]>>1)&0x1F]
	dst[51] = encoding[(id[31]<<4)&0x1F]
}

// decode by unrolling the stdlib base32 algorithm + removing all safe checks
func Decode32(id []byte, src []byte) {
	id[0] = dec[src[0]]<<3 | dec[src[1]]>>2
	id[1] = dec[src[1]]<<6 | dec[src[2]]<<1 | dec[src[3]]>>4
	id[2] = dec[src[3]]<<4 | dec[src[4]]>>1
	id[3] = dec[src[4]]<<7 | dec[src[5]]<<2 | dec[src[6]]>>3
	id[4] = dec[src[6]]<<5 | dec[src[7]]
	id[5] = dec[src[8]]<<3 | dec[src[9]]>>2
	id[6] = dec[src[9]]<<6 | dec[src[10]]<<1 | dec[src[11]]>>4
	id[7] = dec[src[11]]<<4 | dec[src[12]]>>1
	id[8] = dec[src[12]]<<7 | dec[src[13]]<<2 | dec[src[14]]>>3
	id[9] = dec[src[14]]<<5 | dec[src[15]]
	id[10] = dec[src[16]]<<3 | dec[src[17]]>>2
	id[11] = dec[src[17]]<<6 | dec[src[18]]<<1 | dec[src[19]]>>4
	id[12] = dec[src[19]]<<4 | dec[src[20]]>>1
	id[13] = dec[src[20]]<<7 | dec[src[21]]<<2 | dec[src[22]]>>3
	id[14] = dec[src[22]]<<5 | dec[src[23]]
	id[15] = dec[src[24]]<<3 | dec[src[25]]>>2
	id[16] = dec[src[25]]<<6 | dec[src[26]]<<1 | dec[src[27]]>>4
	id[17] = dec[src[27]]<<4 | dec[src[28]]>>1
	id[18] = dec[src[28]]<<7 | dec[src[29]]<<2 | dec[src[30]]>>3
	id[19] = dec[src[30]]<<5 | dec[src[31]]
	id[20] = dec[src[32]]<<3 | dec[src[33]]>>2
	id[21] = dec[src[33]]<<6 | dec[src[34]]<<1 | dec[src[35]]>>4
	id[22] = dec[src[35]]<<4 | dec[src[36]]>>1
	id[23] = dec[src[36]]<<7 | dec[src[37]]<<2 | dec[src[38]]>>3
	id[24] = dec[src[38]]<<5 | dec[src[39]]
	id[25] = dec[src[40]]<<3 | dec[src[41]]>>2
	id[26] = dec[src[41]]<<6 | dec[src[42]]<<1 | dec[src[43]]>>4
	id[27] = dec[src[43]]<<4 | dec[src[44]]>>1
	id[28] = dec[src[44]]<<7 | dec[src[45]]<<2 | dec[src[46]]>>3
	id[29] = dec[src[46]]<<5 | dec[src[47]]
	id[30] = dec[src[48]]<<3 | dec[src[49]]>>2
	id[31] = dec[src[49]]<<6 | dec[src[50]]<<1 | dec[src[51]]>>4
}
