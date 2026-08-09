#ifndef NORMFS_CRC32C_H
#define NORMFS_CRC32C_H

#include <stddef.h>
#include <stdint.h>

/*
 * CRC32C (Castagnoli), reflected polynomial 0x82F63B78, initial and final
 * xor 0xFFFFFFFF. Check value: normfs_crc32c(0, "123456789", 9) is 0xE3069283.
 *
 * The seed is the running value, so
 *   normfs_crc32c(normfs_crc32c(0, a, la), b, lb)
 * equals the checksum of a concatenated with b. Inversion is handled inside,
 * callers always seed with 0.
 *
 * normfs_crc32c uses the CPU instruction: x86_64 builds require SSE4.2 and
 * aarch64 builds require the CRC extension. A CPU without it faults rather
 * than running slower, so there is no second implementation to fall back to.
 *
 * WP proves normfs_crc32c against an assumed contract for the intrinsic path,
 * which no prover can discharge; test_crc32c.c is what checks that assumption,
 * against a reference it builds from the polynomial itself.
 */

/*
 * Declared, not defined: the logic below reads it to say what CRC32C is, and
 * nothing reads it at run time. The values are therefore not pinned here, and
 * what ties normfs_crc32c_logic to Castagnoli is the check-vector test.
 */
extern const uint32_t normfs_crc32c_table[256];

/* The checksum is a recursive definition over the table, not an axiom. */
/*@ axiomatic NormfsCrc32c {
      logic integer normfs_crc32c_step(integer c, integer b) =
        normfs_crc32c_table[(c ^ b) & 0xFF] ^ (c >> 8);

      logic integer normfs_crc32c_fold{L}(integer c, uint8_t *data, integer n) =
        n <= 0
          ? c
          : normfs_crc32c_step(normfs_crc32c_fold(c, data, n - 1),
                               data[n - 1]);

      logic integer normfs_crc32c_logic{L}(integer seed, uint8_t *data,
                                           integer len) =
        0xFFFFFFFF ^ normfs_crc32c_fold(0xFFFFFFFF ^ seed, data, len);
    }
*/

/* Proven lemma (not an axiom): the loop advances its invariant one byte. */
/*@ lemma normfs_crc32c_fold_extend{L}:
      \forall integer c, uint8_t *data, integer n;
        n >= 0 ==>
        normfs_crc32c_fold(c, data, n + 1) ==
          normfs_crc32c_step(normfs_crc32c_fold(c, data, n), data[n]);
*/

/*@ requires len == 0 || \valid_read(data + (0 .. len - 1));
    assigns \nothing;
    ensures \result == normfs_crc32c_logic(crc, data, len);
*/
uint32_t normfs_crc32c(uint32_t crc, const uint8_t *data, size_t len);

#endif /* NORMFS_CRC32C_H */
