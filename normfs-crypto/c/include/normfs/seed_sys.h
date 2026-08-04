#ifndef NORMFS_SEED_SYS_H
#define NORMFS_SEED_SYS_H

#include <stddef.h>
#include <stdint.h>

#include "normfs/seed.h"

/*
 * The syscall boundary. Implemented in src/seed_sys.c, which Frama-C never
 * sees, so WP assumes these contracts and proves src/seed.c against them -- the
 * same trade the CRC32C intrinsic makes in normfs-wal. test_seed.c checks each
 * one against a real filesystem.
 *
 * src/seed.c must never include a system header. Frama-C does ship contracts
 * for open, read, write, fsync, close and getentropy, and using them would
 * replace provable obligations with impossible ones: none of them assigns errno
 * (share/libc/errno.h defines it as __fc_errno, which no syscall contract
 * touches), so every os_error postcondition would fail; read's bound on its
 * result hides behind `assumes Frama_C_entropy_source`, an EVA oracle that is
 * unconstrained under WP; and explicit_bzero is absent from that libc entirely.
 *
 * Paths are (pointer, length) with an explicit NUL precondition rather than
 * valid_read_string, which needs those same string axiomatics. Results are long
 * rather than ssize_t so this header needs no <sys/types.h>; both targets are
 * LP64, which is why -machdep gcc_x86_64 is pinned.
 *
 * EINTR is retried inside the bodies, so no caller observes it. That is part of
 * what these contracts claim.
 */

#define NORMFS_SEED_SYS_ENTROPY_MAX 256   /* getentropy(2) refuses more */
#define NORMFS_SEED_SYS_IO_MAX 0x7FFFFFFF /* keeps the size_t -> long casts in range */

/* Silent about buf on failure: getentropy leaves it unspecified, so
 * normfs_seed_generate wipes it rather than pretend otherwise. */
/*@ requires len <= NORMFS_SEED_SYS_ENTROPY_MAX;
    requires len == 0 || \valid(buf + (0 .. len - 1));
    requires \valid(os_error);
    requires len == 0 || \separated(os_error, buf + (0 .. len - 1));
    assigns buf[0 .. len - 1], *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_seed_sys_entropy(uint8_t *buf, size_t len, int *os_error);

/* open(path, O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC, 0600). */
/*@ requires path_len <= NORMFS_SEED_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires \valid(os_error);
    requires \separated(os_error, path + (0 .. path_len));
    assigns *os_error;
    ensures \result >= 0 || \result == -1;
    ensures \result >= 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_seed_sys_create_excl(const char *path, size_t path_len,
    int *os_error);

/* open(path, O_RDONLY|O_CLOEXEC). */
/*@ requires path_len <= NORMFS_SEED_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires \valid(os_error);
    requires \separated(os_error, path + (0 .. path_len));
    assigns *os_error;
    ensures \result >= 0 || \result == -1;
    ensures \result >= 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_seed_sys_open_read(const char *path, size_t path_len, int *os_error);

/*
 * `-1 <= \result <= (long)len` is the load bearing clause of this header: it is
 * what makes the caller's loop variant decrease and what proves the loop cannot
 * write past buf[len - 1].
 */
/*@ requires len <= NORMFS_SEED_SYS_IO_MAX;
    requires len == 0 || \valid(buf + (0 .. len - 1));
    requires \valid(os_error);
    requires len == 0 || \separated(os_error, buf + (0 .. len - 1));
    assigns buf[0 .. len - 1], *os_error;
    ensures -1 <= \result <= (long)len;
    ensures \result >= 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
long normfs_seed_sys_read(int fd, uint8_t *buf, size_t len, int *os_error);

/* `len > 0 ==> \result != 0` keeps the caller's loop from spinning on a write
 * that reports no progress; the body turns that into a failure. */
/*@ requires len <= NORMFS_SEED_SYS_IO_MAX;
    requires len == 0 || \valid_read(buf + (0 .. len - 1));
    requires \valid(os_error);
    requires len == 0 || \separated(os_error, buf + (0 .. len - 1));
    assigns *os_error;
    ensures -1 <= \result <= (long)len;
    ensures len > 0 ==> \result != 0;
    ensures \result >= 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
long normfs_seed_sys_write(int fd, const uint8_t *buf, size_t len,
    int *os_error);

/*@ requires \valid(os_error);
    assigns *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_seed_sys_fsync(int fd, int *os_error);

/*@ requires \valid(os_error);
    assigns *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_seed_sys_close(int fd, int *os_error);

/*@ requires path_len <= NORMFS_SEED_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    assigns \nothing;
    ensures \result == 0 || \result == 1;
*/
int normfs_seed_sys_exists(const char *path, size_t path_len);

/*
 * What is assumed here is not the postcondition but the reason the function
 * exists: that the compiler did not elide stores into an object about to die.
 */
/*@ requires len == 0 || \valid(buf + (0 .. len - 1));
    assigns buf[0 .. len - 1];
    ensures \forall integer k; 0 <= k < len ==> buf[k] == 0;
*/
void normfs_seed_sys_zero(uint8_t *buf, size_t len);

#endif /* NORMFS_SEED_SYS_H */
