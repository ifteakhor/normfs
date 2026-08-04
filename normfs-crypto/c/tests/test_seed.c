/*
 * The runtime half of the seed module's correctness argument: src/seed.c is
 * proved by WP, but every syscall it reaches is an assumed contract in
 * normfs/seed_sys.h. These tests discharge them. They also pin the literal
 * ".crypto_seed", whose length the ACSL fixes but whose bytes it does not.
 */
#define _POSIX_C_SOURCE 200809L
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE 1
#endif

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "normfs/seed.h"

/* assert() is a no-op under NDEBUG, which the Release build defines, so the
 * checks report and return failure themselves instead. */
#define CHECK(cond)                                                     \
	do {                                                            \
		if (!(cond)) {                                          \
			fprintf(stderr, "seed: FAIL %s:%d: %s\n",       \
			    __FILE__, __LINE__, #cond);                 \
			return 1;                                       \
		}                                                       \
	} while (0)

#define DIR_ROUNDTRIP "roundtrip"
#define DIR_TWICE "twice"
#define DIR_MODE "mode"
#define DIR_SHORT "shortfile"
#define DIR_MISSING "missing"
#define DIR_EXISTS "exists"

static const char *const test_dirs[] = {
	DIR_ROUNDTRIP, DIR_TWICE, DIR_MODE, DIR_SHORT, DIR_MISSING,
	DIR_EXISTS, NULL
};

static char root[512];

/* Registered with atexit, so a failing CHECK's early return still cleans up. */
static void
cleanup_root(void)
{
	char path[768];
	size_t i;

	if (root[0] == '\0')
		return;

	for (i = 0u; test_dirs[i] != NULL; i++) {
		(void)snprintf(path, sizeof(path), "%s/%s/%s", root,
		    test_dirs[i], NORMFS_SEED_FILE_NAME);
		(void)unlink(path);
		(void)snprintf(path, sizeof(path), "%s/%s", root,
		    test_dirs[i]);
		(void)rmdir(path);
	}
	(void)rmdir(root);
}

static int
make_dir(const char *name, char *out, size_t out_len)
{
	(void)snprintf(out, out_len, "%s/%s", root, name);
	return mkdir(out, 0700);
}

static void
fill(uint8_t *buf, size_t len, uint8_t base)
{
	size_t i;

	for (i = 0u; i < len; i++)
		buf[i] = (uint8_t)(base + (uint8_t)i);
}

static int
is_all_zero(const uint8_t *buf, size_t len)
{
	size_t i;

	for (i = 0u; i < len; i++) {
		if (buf[i] != 0u)
			return 0;
	}
	return 1;
}

static int
test_path_join(void)
{
	char out[64];
	size_t used = 0u;

	CHECK(normfs_seed_path("/tmp/data", 9u, out, sizeof(out), &used) ==
	    NORMFS_SEED_OK);
	CHECK(used == 9u + 1u + (size_t)NORMFS_SEED_FILE_NAME_LEN);
	CHECK(strcmp(out, "/tmp/data/.crypto_seed") == 0);
	CHECK(out[used] == '\0');
	return 0;
}

static int
test_path_trailing_slash(void)
{
	char out[64];
	size_t used = 0u;

	CHECK(normfs_seed_path("/", 1u, out, sizeof(out), &used) ==
	    NORMFS_SEED_OK);
	CHECK(strcmp(out, "/.crypto_seed") == 0);

	CHECK(normfs_seed_path("/tmp/", 5u, out, sizeof(out), &used) ==
	    NORMFS_SEED_OK);
	CHECK(strcmp(out, "/tmp/.crypto_seed") == 0);
	return 0;
}

static int
test_path_empty_dir_stays_relative(void)
{
	char out[64];
	size_t used = 0u;

	/* Matches Rust's Path::new("").join(), which is relative not rooted. */
	CHECK(normfs_seed_path("", 0u, out, sizeof(out), &used) ==
	    NORMFS_SEED_OK);
	CHECK(strcmp(out, ".crypto_seed") == 0);
	CHECK(used == (size_t)NORMFS_SEED_FILE_NAME_LEN);
	return 0;
}

static int
test_path_too_long(void)
{
	char out[64];
	size_t used = 1u;
	size_t need;
	size_t i;

	/* "/tmp/data" + "/" + ".crypto_seed" + NUL */
	need = 9u + 1u + (size_t)NORMFS_SEED_FILE_NAME_LEN + 1u;

	memset(out, 0x5A, sizeof(out));
	CHECK(normfs_seed_path("/tmp/data", 9u, out, need - 1u, &used) ==
	    NORMFS_SEED_ERR_PATH_TOO_LONG);
	CHECK(used == 0u);
	/* Refusing means writing nothing, not writing a truncated path. */
	for (i = 0u; i < sizeof(out); i++)
		CHECK((unsigned char)out[i] == 0x5Au);

	CHECK(normfs_seed_path("/tmp/data", 9u, out, need, &used) ==
	    NORMFS_SEED_OK);
	CHECK(used == need - 1u);
	return 0;
}

static int
test_generate_is_entropy(void)
{
	uint8_t a[NORMFS_SEED_SIZE];
	uint8_t b[NORMFS_SEED_SIZE];
	struct normfs_seed_result r;

	r = normfs_seed_generate(a, sizeof(a));
	CHECK(r.status == NORMFS_SEED_OK);
	CHECK(r.os_error == 0);

	r = normfs_seed_generate(b, sizeof(b));
	CHECK(r.status == NORMFS_SEED_OK);

	CHECK(memcmp(a, b, sizeof(a)) != 0);
	CHECK(!is_all_zero(a, sizeof(a)));
	CHECK(!is_all_zero(b, sizeof(b)));
	return 0;
}

static int
test_generate_rejects_wrong_size(void)
{
	uint8_t buf[NORMFS_SEED_SIZE];
	struct normfs_seed_result r;

	fill(buf, sizeof(buf), 0x11u);
	r = normfs_seed_generate(buf, sizeof(buf) - 1u);
	CHECK(r.status == NORMFS_SEED_ERR_INVALID_ARG);
	CHECK(r.os_error == 0);
	CHECK(is_all_zero(buf, sizeof(buf)));
	return 0;
}

/* Fails if the wipe's stores were optimised away, on whichever fallback this
 * platform compiled. */
static int
test_zero_wipes(void)
{
	uint8_t buf[64];

	fill(buf, sizeof(buf), 0xA5u);
	CHECK(!is_all_zero(buf, sizeof(buf)));
	normfs_seed_zero(buf, sizeof(buf));
	CHECK(is_all_zero(buf, sizeof(buf)));
	return 0;
}

static int
test_save_then_load_roundtrip(void)
{
	char dir[640];
	uint8_t in[NORMFS_SEED_SIZE];
	uint8_t out[NORMFS_SEED_SIZE];
	struct normfs_seed_result r;

	CHECK(make_dir(DIR_ROUNDTRIP, dir, sizeof(dir)) == 0);
	fill(in, sizeof(in), 0x40u);

	r = normfs_seed_save(dir, strlen(dir), in, sizeof(in));
	CHECK(r.status == NORMFS_SEED_OK);
	CHECK(r.os_error == 0);

	memset(out, 0xFF, sizeof(out));
	r = normfs_seed_load(dir, strlen(dir), out, sizeof(out));
	CHECK(r.status == NORMFS_SEED_OK);
	CHECK(memcmp(in, out, sizeof(in)) == 0);
	return 0;
}

static int
test_second_save_fails(void)
{
	char dir[640];
	uint8_t first[NORMFS_SEED_SIZE];
	uint8_t second[NORMFS_SEED_SIZE];
	uint8_t out[NORMFS_SEED_SIZE];
	struct normfs_seed_result r;

	CHECK(make_dir(DIR_TWICE, dir, sizeof(dir)) == 0);
	fill(first, sizeof(first), 0x01u);
	fill(second, sizeof(second), 0x80u);

	r = normfs_seed_save(dir, strlen(dir), first, sizeof(first));
	CHECK(r.status == NORMFS_SEED_OK);

	/* A second writer must lose, because every byte already on disk is
	 * encrypted under the first seed. */
	r = normfs_seed_save(dir, strlen(dir), second, sizeof(second));
	CHECK(r.status == NORMFS_SEED_ERR_IO);
	CHECK(r.os_error == EEXIST);

	r = normfs_seed_load(dir, strlen(dir), out, sizeof(out));
	CHECK(r.status == NORMFS_SEED_OK);
	CHECK(memcmp(first, out, sizeof(first)) == 0);
	return 0;
}

static int
test_file_mode_is_0600(void)
{
	char dir[640];
	char path[768];
	uint8_t seed[NORMFS_SEED_SIZE];
	struct normfs_seed_result r;
	struct stat st;

	CHECK(make_dir(DIR_MODE, dir, sizeof(dir)) == 0);
	fill(seed, sizeof(seed), 0x22u);

	r = normfs_seed_save(dir, strlen(dir), seed, sizeof(seed));
	CHECK(r.status == NORMFS_SEED_OK);

	(void)snprintf(path, sizeof(path), "%s/%s", dir, NORMFS_SEED_FILE_NAME);
	CHECK(stat(path, &st) == 0);
	CHECK((st.st_mode & 07777) == 0600);
	return 0;
}

static int
test_load_short_file(void)
{
	char dir[640];
	char path[768];
	uint8_t out[NORMFS_SEED_SIZE];
	struct normfs_seed_result r;
	int fd;

	CHECK(make_dir(DIR_SHORT, dir, sizeof(dir)) == 0);
	(void)snprintf(path, sizeof(path), "%s/%s", dir, NORMFS_SEED_FILE_NAME);

	fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
	CHECK(fd >= 0);
	CHECK(write(fd, "0123456789ABCDEF", 16) == 16);
	CHECK(close(fd) == 0);

	memset(out, 0xFF, sizeof(out));
	r = normfs_seed_load(dir, strlen(dir), out, sizeof(out));
	CHECK(r.status == NORMFS_SEED_ERR_INVALID_SEED);
	CHECK(is_all_zero(out, sizeof(out)));
	return 0;
}

static int
test_load_missing(void)
{
	char dir[640];
	uint8_t out[NORMFS_SEED_SIZE];
	struct normfs_seed_result r;

	CHECK(make_dir(DIR_MISSING, dir, sizeof(dir)) == 0);

	memset(out, 0xFF, sizeof(out));
	r = normfs_seed_load(dir, strlen(dir), out, sizeof(out));
	CHECK(r.status == NORMFS_SEED_ERR_IO);
	/* Without a real ENOENT, from_raw_os_error cannot reproduce
	 * ErrorKind::NotFound on the Rust side. */
	CHECK(r.os_error == ENOENT);
	CHECK(is_all_zero(out, sizeof(out)));
	return 0;
}

static int
test_exists(void)
{
	char dir[640];
	uint8_t seed[NORMFS_SEED_SIZE];
	struct normfs_seed_result r;

	CHECK(make_dir(DIR_EXISTS, dir, sizeof(dir)) == 0);
	CHECK(normfs_seed_exists(dir, strlen(dir)) == 0);

	fill(seed, sizeof(seed), 0x33u);
	r = normfs_seed_save(dir, strlen(dir), seed, sizeof(seed));
	CHECK(r.status == NORMFS_SEED_OK);

	CHECK(normfs_seed_exists(dir, strlen(dir)) == 1);
	return 0;
}

int
main(void)
{
	const char *tmp = getenv("TMPDIR");

	/* So test_file_mode_is_0600 asserts the requested mode rather than the
	 * build environment's umask. */
	(void)umask(0);

	(void)snprintf(root, sizeof(root), "%s/normfs_seed_XXXXXX",
	    (tmp != NULL && tmp[0] != '\0') ? tmp : "/tmp");
	if (mkdtemp(root) == NULL) {
		perror("seed: mkdtemp");
		return 1;
	}
	if (atexit(cleanup_root) != 0) {
		fprintf(stderr, "seed: atexit failed\n");
		cleanup_root();
		return 1;
	}

	if (test_path_join() != 0)
		return 1;
	if (test_path_trailing_slash() != 0)
		return 1;
	if (test_path_empty_dir_stays_relative() != 0)
		return 1;
	if (test_path_too_long() != 0)
		return 1;
	if (test_generate_is_entropy() != 0)
		return 1;
	if (test_generate_rejects_wrong_size() != 0)
		return 1;
	if (test_zero_wipes() != 0)
		return 1;
	if (test_save_then_load_roundtrip() != 0)
		return 1;
	if (test_second_save_fails() != 0)
		return 1;
	if (test_file_mode_is_0600() != 0)
		return 1;
	if (test_load_short_file() != 0)
		return 1;
	if (test_load_missing() != 0)
		return 1;
	if (test_exists() != 0)
		return 1;

	printf("seed: all tests passed\n");
	return 0;
}
