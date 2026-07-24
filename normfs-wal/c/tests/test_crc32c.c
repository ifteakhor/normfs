#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "normfs/crc32c.h"

#define NORMFS_CRC32C_POLY 0x82F63B78U

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

static uint32_t rng_state = 0x12345678U;

static uint8_t
rng_next(void)
{
	rng_state ^= rng_state << 13;
	rng_state ^= rng_state >> 17;
	rng_state ^= rng_state << 5;
	return (uint8_t)(rng_state & 0xFFu);
}

static void
test_check_vector(void)
{
	const uint8_t input[] = {'1', '2', '3', '4', '5', '6', '7', '8', '9'};

	assert(normfs_crc32c(0u, input, sizeof(input)) == 0xE3069283U);
	assert(normfs_crc32c_portable(0u, input, sizeof(input)) == 0xE3069283U);
}

static void
test_empty_input(void)
{
	const uint8_t input[1] = {0};

	assert(normfs_crc32c(0u, input, 0u) == 0u);
	assert(normfs_crc32c_portable(0u, input, 0u) == 0u);
}

static void
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
		assert(whole == joined);
	}
}

static void
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
			assert(normfs_crc32c_portable(0u, p, len) == expected);
			assert(normfs_crc32c(0u, p, len) == expected);
		}
	}
}

static void
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
			assert(normfs_crc32c(seeds[s], buf, i) == expected);
			assert(normfs_crc32c_portable(seeds[s], buf, i) ==
			    expected);
		}
	}
}

int
main(void)
{
	reference_table_init();

	test_check_vector();
	test_empty_input();
	test_seed_composition();
	test_matches_reference_and_dispatch();
	test_seeded_matches_reference();

	printf("crc32c: all tests passed\n");
	return 0;
}
