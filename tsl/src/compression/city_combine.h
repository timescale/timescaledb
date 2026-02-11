/* The code below is taken from the CityHash project: https://github.com/google/cityhash
 * specifically from here: https://github.com/google/cityhash/blob/master/src/city.h#L101
 */

/*
 * Copyright (c) 2011 Google, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * CityHash, by Geoff Pike and Jyrki Alakuijala
 *
 * http://code.google.com/p/cityhash/
 */

#pragma once

#include <stdint.h>

static inline uint64_t
city_hash_combine(uint64_t accumulated_hash, uint64_t new_hash)
{
	const uint64_t kMul = 0x9ddfea08eb382d69ULL;
	uint64_t a = (accumulated_hash ^ new_hash) * kMul;
	a ^= (a >> 47);
	uint64_t b = (new_hash ^ a) * kMul;
	b ^= (b >> 47);
	b *= kMul;
	return b;
}
