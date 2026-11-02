package sstable

import "encoding/binary"

// Bloom filter layout: [k u8][nbits u32 LE][bits ...]. Hashing uses a
// 64-bit FNV-1a style mix split into two 32-bit halves and combined with
// Kirsch-Mitzenmacher double hashing, so one hash computation serves all k
// probes. The hash is deterministic across processes, which matters because
// filters are persisted.

func hash64(b []byte) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime
	}
	// Final avalanche (from splitmix64) so the low bits are well mixed.
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}

// BuildBloom returns a filter for keys with roughly bitsPerKey bits each.
func BuildBloom(keys [][]byte, bitsPerKey int) []byte {
	if bitsPerKey < 1 {
		bitsPerKey = 1
	}
	k := int(float64(bitsPerKey) * 0.69) // ln 2
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	nbits := len(keys) * bitsPerKey
	if nbits < 64 {
		nbits = 64
	}
	nbytes := (nbits + 7) / 8
	nbits = nbytes * 8
	out := make([]byte, 5+nbytes)
	out[0] = byte(k)
	binary.LittleEndian.PutUint32(out[1:5], uint32(nbits))
	bits := out[5:]
	for _, key := range keys {
		h := hash64(key)
		h1, h2 := uint32(h), uint32(h>>32)|1
		for i := 0; i < k; i++ {
			pos := (h1 + uint32(i)*h2) % uint32(nbits)
			bits[pos/8] |= 1 << (pos % 8)
		}
	}
	return out
}

// BloomMayContain reports whether key may be in the set the filter encodes.
func BloomMayContain(filter, key []byte) bool {
	if len(filter) < 5 {
		return true
	}
	k := int(filter[0])
	nbits := binary.LittleEndian.Uint32(filter[1:5])
	bits := filter[5:]
	if nbits == 0 || len(bits)*8 < int(nbits) {
		return true
	}
	h := hash64(key)
	h1, h2 := uint32(h), uint32(h>>32)|1
	for i := 0; i < k; i++ {
		pos := (h1 + uint32(i)*h2) % nbits
		if bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}
