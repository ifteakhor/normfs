#include <stdio.h>
#include <string.h>

#include "normfs/crc32c.h"

#define NORMFS_CRC32C_POLY 0x82F63B78U

/* assert() is a no-op under NDEBUG, which the Release build defines, so the
 * checks report and return failure themselves instead. */
#define CHECK(cond)                                                     \
	do {                                                            \
		if (!(cond)) {                                          \
			fprintf(stderr, "crc32c: FAIL %s:%d: %s\n",     \
			    __FILE__, __LINE__, #cond);                 \
			return 1;                                       \
		}                                                       \
	} while (0)

static uint32_t reference_table[256];

static void
reference_table_init(void)
{
	uint32_t n;

	for (n = 0u; n < 256u; n++) {
		uint32_t c = n;
		int k;
		for (k = 0; k < 8; k++) {
			c = (c & 1u) ? ((c >> 1) ^ NORMFS_CRC32C_POLY) : (c >> 1);
		}
		reference_table[n] = c;
	}
}

static uint32_t
reference_crc32c(uint32_t crc, const uint8_t *data, size_t len)
{
	uint32_t c = ~crc;
	size_t i;

	for (i = 0u; i < len; i++) {
		c = reference_table[(c ^ data[i]) & 0xFFu] ^ (c >> 8);
	}

	return ~c;
}

/* crc32c.c internals, deliberately absent from the public header; a typo
 * here fails at link time. */
extern const uint32_t normfs_crc32c_shift_1024[32];
extern const uint32_t normfs_crc32c_shift_2048[32];

static uint32_t
matrix_apply(const uint32_t m[32], uint32_t v)
{
	uint32_t r = 0u;
	uint32_t i;

	for (i = 0u; i < 32u; i++) {
		if ((v >> i) & 1u)
			r ^= m[i];
	}
	return r;
}

static void
matrix_square(const uint32_t m[32], uint32_t out[32])
{
	uint32_t i;

	for (i = 0u; i < 32u; i++)
		out[i] = matrix_apply(m, m[i]);
}

static uint32_t rng_state = 0x12345678U;

static uint8_t
rng_next(void)
{
	rng_state ^= rng_state << 13;
	rng_state ^= rng_state >> 17;
	rng_state ^= rng_state << 5;
	return (uint8_t)(rng_state & 0xFFu);
}

static int
test_check_vector(void)
{
	const uint8_t input[] = {'1', '2', '3', '4', '5', '6', '7', '8', '9'};

	CHECK(normfs_crc32c(0u, input, sizeof(input)) == 0xE3069283U);
	CHECK(normfs_crc32c_portable(0u, input, sizeof(input)) == 0xE3069283U);
	return 0;
}

static int
test_empty_input(void)
{
	const uint8_t input[1] = {0};

	CHECK(normfs_crc32c(0u, input, 0u) == 0u);
	CHECK(normfs_crc32c_portable(0u, input, 0u) == 0u);
	return 0;
}

static int
test_seed_composition(void)
{
	uint8_t buf[64];
	size_t split;
	size_t i;

	for (i = 0u; i < sizeof(buf); i++) buf[i] = rng_next();

	for (split = 0u; split <= sizeof(buf); split++) {
		uint32_t whole = normfs_crc32c(0u, buf, sizeof(buf));
		uint32_t part = normfs_crc32c(0u, buf, split);
		uint32_t joined = normfs_crc32c(part, buf + split,
		    sizeof(buf) - split);
		CHECK(whole == joined);
	}
	return 0;
}

static int
test_matches_reference_and_dispatch(void)
{
	static uint8_t buf[1024 + 8];
	size_t len;
	size_t align;
	size_t i;

	for (i = 0u; i < sizeof(buf); i++) buf[i] = rng_next();

	for (len = 0u; len <= 1024u; len++) {
		for (align = 0u; align < 8u; align++) {
			const uint8_t *p = buf + align;
			uint32_t expected = reference_crc32c(0u, p, len);
			CHECK(normfs_crc32c_portable(0u, p, len) == expected);
			CHECK(normfs_crc32c(0u, p, len) == expected);
		}
	}
	return 0;
}

static int
test_seeded_matches_reference(void)
{
	static uint8_t buf[257];
	uint32_t seeds[] = {0u, 1u, 0xFFFFFFFFU, 0xDEADBEEFU};
	size_t s;
	size_t i;

	for (i = 0u; i < sizeof(buf); i++) buf[i] = rng_next();

	for (s = 0u; s < sizeof(seeds) / sizeof(seeds[0]); s++) {
		for (i = 0u; i <= 256u; i++) {
			uint32_t expected = reference_crc32c(seeds[s], buf, i);
			CHECK(normfs_crc32c(seeds[s], buf, i) == expected);
			CHECK(normfs_crc32c_portable(seeds[s], buf, i) ==
			    expected);
		}
	}
	return 0;
}

/* The shipped shift matrices must equal the operator "append 2^s zero bytes"
 * derived from the polynomial alone: squaring the one-zero-byte operator s
 * times. Any wrong entry in either matrix fails here. */
static int
test_shift_matrices(void)
{
	uint32_t m[32];
	uint32_t sq[32];
	uint32_t i;
	uint32_t s;

	/* One zero byte: step(c, 0) = table[c & 0xFF] ^ (c >> 8), so column i
	 * is the image of basis bit i; the index masks to 0 for i >= 8. */
	for (i = 0u; i < 32u; i++) {
		uint32_t bit = 1u << i;
		m[i] = reference_table[bit & 0xFFu] ^ (bit >> 8);
	}

	for (s = 1u; s <= 11u; s++) {
		matrix_square(m, sq);
		memcpy(m, sq, sizeof(m));
		if (s == 10u) {
			for (i = 0u; i < 32u; i++)
				CHECK(m[i] == normfs_crc32c_shift_1024[i]);
		}
		if (s == 11u) {
			for (i = 0u; i < 32u; i++)
				CHECK(m[i] == normfs_crc32c_shift_2048[i]);
		}
	}
	return 0;
}

/* Sweeps every stream-block boundary and tail size of the chunked fast path
 * at every 8-byte misalignment. Together with the 0..1024 sweep above, every
 * length 0..6144 is covered. */
static int
test_streamed_matches_reference(void)
{
	static uint8_t buf[6144 + 8];
	uint32_t seeds[] = {0u, 0xDEADBEEFU};
	size_t s;
	size_t len;
	size_t align;
	size_t i;

	for (i = 0u; i < sizeof(buf); i++) buf[i] = rng_next();

	for (s = 0u; s < sizeof(seeds) / sizeof(seeds[0]); s++) {
		for (len = 1024u; len <= 6144u; len++) {
			for (align = 0u; align < 8u; align++) {
				const uint8_t *p = buf + align;
				uint32_t expected =
				    reference_crc32c(seeds[s], p, len);
				CHECK(normfs_crc32c(seeds[s], p, len) ==
				    expected);
				CHECK(normfs_crc32c_portable(seeds[s], p, len)
				    == expected);
			}
		}
	}
	return 0;
}

/* The first stream of a chunk continues the running state, so splitting at a
 * boundary near the chunk thresholds must compose like any other split. */
static int
test_seed_composition_streamed(void)
{
	static uint8_t buf[6144];
	size_t splits[] = {0u, 1u, 7u, 255u, 767u, 768u, 1024u, 3071u,
	    3072u, 3073u, 4096u, 6144u};
	uint32_t whole;
	size_t s;
	size_t i;

	for (i = 0u; i < sizeof(buf); i++) buf[i] = rng_next();

	whole = normfs_crc32c(0u, buf, sizeof(buf));

	for (s = 0u; s < sizeof(splits) / sizeof(splits[0]); s++) {
		size_t split = splits[s];
		uint32_t part = normfs_crc32c(0u, buf, split);
		CHECK(normfs_crc32c(part, buf + split, sizeof(buf) - split) ==
		    whole);
	}
	return 0;
}

int
main(void)
{
	reference_table_init();

	if (test_check_vector() != 0) return 1;
	if (test_empty_input() != 0) return 1;
	if (test_seed_composition() != 0) return 1;
	if (test_matches_reference_and_dispatch() != 0) return 1;
	if (test_seeded_matches_reference() != 0) return 1;
	if (test_shift_matrices() != 0) return 1;
	if (test_streamed_matches_reference() != 0) return 1;
	if (test_seed_composition_streamed() != 0) return 1;

	printf("crc32c: all tests passed\n");
	return 0;
}
