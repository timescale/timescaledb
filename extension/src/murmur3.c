//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

// Note - The x86 and x64 versions do _not_ produce the same results, as the
// algorithms are optimized for their respective platforms. You can still
// compile and run any of them on any platform, but your performance with the
// non-native version will be less than optimal.

#include "pgmurmur3.h"

//-----------------------------------------------------------------------------
// Platform-specific functions and macros

static inline uint32_t rotl32(uint32_t x, int8_t r)
{
	 return (x << r) | (x >> (32 - r));
}

#define	ROTL32(x,y)	rotl32(x,y)

//-----------------------------------------------------------------------------
// Block read - if your platform needs to do endian-swapping or can only
// handle aligned reads, do the conversion here
static inline uint32_t getblock(const uint32_t * p, int i)
{
	 return p[i];
}

//-----------------------------------------------------------------------------
// Finalization mix - force all bits of a hash block to avalanche
static inline uint32_t fmix(uint32_t h)
{
	 h ^= h >> 16;
	 h *= 0x85ebca6b;
	 h ^= h >> 13;
	 h *= 0xc2b2ae35;
	 h ^= h >> 16;
	 return h;
}

//-----------------------------------------------------------------------------
void hlib_murmur3(const void *key, size_t len, uint64_t *io)
{
	 const uint8_t *data = (const uint8_t *) key;
	 const int nblocks = len / 4;
	 uint32_t h1 = io[0];
	 uint32_t c1 = 0xcc9e2d51;
	 uint32_t c2 = 0x1b873593;
	 const uint32_t *blocks;
	 const uint8_t *tail;
	 int i;
	 uint32_t k1;

	 //----------
	 // body
	 blocks = (const uint32_t *) (data + nblocks * 4);
	 for (i = -nblocks; i; i++) {
		  k1 = getblock(blocks, i);
		  k1 *= c1;
		  k1 = ROTL32(k1, 15);
		  k1 *= c2;
		  h1 ^= k1;
		  h1 = ROTL32(h1, 13);
		  h1 = h1 * 5 + 0xe6546b64;
	 }
	 //----------
	 // tail
	 tail = (const uint8_t *) (data + nblocks * 4);
	 k1 = 0;
	 switch (len & 3) {
	 case 3:
		  k1 ^= tail[2] << 16;
	 case 2:
		  k1 ^= tail[1] << 8;
	 case 1:
		  k1 ^= tail[0];
		  k1 *= c1;
		  k1 = ROTL32(k1, 15);
		  k1 *= c2;
		  h1 ^= k1;
	 };

	 //----------
	 // finalization
	 h1 ^= len;
	 h1 = fmix(h1);
	 io[0] = h1;
}

