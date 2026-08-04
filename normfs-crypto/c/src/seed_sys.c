/*
 * The syscall bodies behind the assumed contracts in normfs/seed_sys.h. Never
 * given to Frama-C, and the only file in the module that includes a system
 * header; seed_sys.h explains why that matters.
 *
 * -std=c99 sets __STRICT_ANSI__, which hides every POSIX declaration used here;
 * getentropy and explicit_bzero are not even POSIX-2008. The macros below must
 * precede every #include.
 */
#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE 1
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE 1
#endif

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/* getentropy: <sys/random.h> on glibc >= 2.25 and on macOS >= 10.12. */
#if defined(__APPLE__) || defined(__GLIBC__)
#include <sys/random.h>
#define NORMFS_SEED_HAVE_GETENTROPY 1
#elif defined(__OpenBSD__) || defined(__FreeBSD__)
#include <unistd.h>
#define NORMFS_SEED_HAVE_GETENTROPY 1
#endif

/*
 * explicit_bzero exists on glibc >= 2.25 and the BSDs, and not on macOS at all,
 * so the volatile store loop below is the only branch guaranteed to compile
 * everywhere. test_zero_wipes is the arbiter.
 */
#if defined(__GLIBC__) && \
    (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 25))
#include <strings.h>
#define NORMFS_SEED_HAVE_EXPLICIT_BZERO 1
#elif defined(__OpenBSD__) || defined(__FreeBSD__) || defined(__NetBSD__)
#include <strings.h>
#define NORMFS_SEED_HAVE_EXPLICIT_BZERO 1
#endif

#include "normfs/seed_sys.h"

/* O_CLOEXEC is POSIX-2008, but define it away rather than fail on a host that
 * predates it: the descriptor's lifetime here is a few statements long. */
#if !defined(O_CLOEXEC)
#define O_CLOEXEC 0
#endif

/* A failing syscall that left errno at 0 would break the Rust side's
 * io::Error::from_raw_os_error, so normalise to EIO. */
static int
normfs_seed_sys_fail(int *os_error)
{
	int e = errno;

	*os_error = (e > 0) ? e : EIO;
	return -1;
}

int
normfs_seed_sys_entropy(uint8_t *buf, size_t len, int *os_error)
{
#if defined(NORMFS_SEED_HAVE_GETENTROPY)
	*os_error = 0;
	if (len == 0u)
		return 0;

	errno = 0;
	if (getentropy(buf, len) != 0)
		return normfs_seed_sys_fail(os_error);

	return 0;
#else
	/* For older glibc, where getentropy arrived in 2.25, more than for
	 * exotic targets. */
	int fd;
	size_t total = 0u;

	*os_error = 0;
	if (len == 0u)
		return 0;

	errno = 0;
	fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
	if (fd < 0)
		return normfs_seed_sys_fail(os_error);

	while (total < len) {
		ssize_t n = read(fd, buf + total, len - total);

		if (n < 0) {
			if (errno == EINTR)
				continue;
			(void)normfs_seed_sys_fail(os_error);
			(void)close(fd);
			return -1;
		}
		if (n == 0) {
			/* urandom never reports EOF; treat it as a broken
			 * entropy source rather than looping forever. */
			*os_error = EIO;
			(void)close(fd);
			return -1;
		}
		total += (size_t)n;
	}

	(void)close(fd);
	return 0;
#endif
}

int
normfs_seed_sys_create_excl(const char *path, size_t path_len, int *os_error)
{
	int fd;

	(void)path_len;
	*os_error = 0;

	errno = 0;
	fd = open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
	if (fd < 0)
		return normfs_seed_sys_fail(os_error);

	return fd;
}

int
normfs_seed_sys_open_read(const char *path, size_t path_len, int *os_error)
{
	int fd;

	(void)path_len;
	*os_error = 0;

	errno = 0;
	fd = open(path, O_RDONLY | O_CLOEXEC);
	if (fd < 0)
		return normfs_seed_sys_fail(os_error);

	return fd;
}

long
normfs_seed_sys_read(int fd, uint8_t *buf, size_t len, int *os_error)
{
	ssize_t n;

	*os_error = 0;
	if (len == 0u)
		return 0L;

	do {
		errno = 0;
		n = read(fd, buf, len);
	} while (n < 0 && errno == EINTR);

	if (n < 0)
		return (long)normfs_seed_sys_fail(os_error);

	return (long)n;
}

long
normfs_seed_sys_write(int fd, const uint8_t *buf, size_t len, int *os_error)
{
	ssize_t n;

	*os_error = 0;
	if (len == 0u)
		return 0L;

	do {
		errno = 0;
		n = write(fd, buf, len);
	} while (n < 0 && errno == EINTR);

	if (n < 0)
		return (long)normfs_seed_sys_fail(os_error);

	/* No progress would spin the caller's loop forever. POSIX permits it
	 * only in corners that do not apply to a regular file. */
	if (n == 0) {
		*os_error = EIO;
		return -1L;
	}

	return (long)n;
}

int
normfs_seed_sys_fsync(int fd, int *os_error)
{
	int rc;

	*os_error = 0;

	do {
		errno = 0;
		rc = fsync(fd);
	} while (rc != 0 && errno == EINTR);

	if (rc != 0)
		return normfs_seed_sys_fail(os_error);

	return 0;
}

int
normfs_seed_sys_close(int fd, int *os_error)
{
	*os_error = 0;

	errno = 0;
	/* Not retried on EINTR: Linux releases the descriptor regardless, so a
	 * retry could close one already handed to another thread. */
	if (close(fd) != 0)
		return normfs_seed_sys_fail(os_error);

	return 0;
}

int
normfs_seed_sys_exists(const char *path, size_t path_len)
{
	struct stat st;

	(void)path_len;

	return stat(path, &st) == 0;
}

void
normfs_seed_sys_zero(uint8_t *buf, size_t len)
{
	if (len == 0u)
		return;

#if defined(NORMFS_SEED_HAVE_EXPLICIT_BZERO)
	explicit_bzero(buf, len);
#elif defined(__STDC_LIB_EXT1__)
	(void)memset_s(buf, len, 0, len);
#else
	{
		volatile uint8_t *p = buf;
		size_t i;

		for (i = 0u; i < len; i++)
			p[i] = 0u;
	}
	/* Volatile stores cannot be elided, but without a barrier the compiler
	 * may still sink them past a later free. */
#if defined(__GNUC__) || defined(__clang__)
	__asm__ __volatile__("" : : "r"(buf) : "memory");
#endif
#endif
}
