#ifndef NORMFS_WAL_RING_H
#define NORMFS_WAL_RING_H

#include <stddef.h>
#include <stdint.h>

#include "normfs/wal_page.h"

/*
 * A ring of WAL pages. Rust allocates one arena of page_count * page_size
 * bytes, initialises each page descriptor (normfs_wal_page_init) over its own
 * slice of it, and hands both to normfs_wal_ring_init; C never allocates.
 *
 * The ring exposes primitives that each touch a single page, so their frames
 * stay precise; Rust sequences them:
 *
 *   r = try_append(record)
 *   if r.status == NEEDS_ROTATE:
 *       f = find_reusable()
 *       if !f.found: buffer is full, flush/wait
 *       rotate_to(f.index)
 *       r = try_append(record)     // an empty page always has room
 *
 * Entry ids run sequentially across pages; the id of the next entry is
 * next_entry_id. Page k is the arena slice at k * page_size, so distinct pages
 * are disjoint by arithmetic and each keeps its own page_wf independently.
 */

enum normfs_wal_ring_status {
	NORMFS_WAL_RING_OK = 0,
	NORMFS_WAL_RING_NEEDS_ROTATE = 1,  /* active page is full */
	NORMFS_WAL_RING_ERR_TOO_LARGE = 2  /* record does not fit an empty page */
};

struct normfs_wal_ring {
	struct normfs_wal_page *pages;
	uint8_t *arena;
	size_t page_count;
	size_t page_size;
	size_t active;
	uint64_t next_entry_id;
	uint64_t next_page_id;
	uint64_t min_essential_id;
};

struct normfs_wal_ring_append_result {
	uint64_t entry_id;
	size_t page_index;
	int status;
};

struct normfs_wal_ring_reusable_result {
	size_t index;
	int found;
};

struct normfs_wal_ring_seek_result {
	size_t page_index;
	uint32_t index;
	int found;
};

/*@ axiomatic NormfsWalRing {
      predicate normfs_wal_ring_pages_wf{L}(struct normfs_wal_ring *r) =
        \forall integer k; 0 <= k < r->page_count ==>
          normfs_wal_page_wf(&r->pages[k]);

      // Each page's cap matches page_size (so an empty page fits any record
      // that fits page_size), and the active page continues the id run.
      predicate normfs_wal_ring_scalar_wf{L}(struct normfs_wal_ring *r) =
        r->page_count >= 1 &&
        r->page_size >= NORMFS_WAL_ENTRY_V1_MIN_SIZE + 4 &&
        r->page_size <= 0xFFFFFFFF &&
        r->active < r->page_count &&
        (\forall integer k; 0 <= k < r->page_count ==>
           r->pages[k].cap == r->page_size) &&
        r->pages[r->active].first_entry_id + (integer)r->pages[r->active].count
          == r->next_entry_id;

      // The pages are slices of one allocation, in index order.
      //
      // This is what makes the pages provably disjoint. Stated the other way --
      // one buffer per page, and a quantifier asserting that no two of them
      // overlap -- disjointness is an axiom the caller supplies, and every
      // function that writes anything has to carry it across itself. That is a
      // \forall over *pairs* of pages, and the automatic provers do not
      // transport it: it is the one goal rotate_to could not discharge. As
      // slices of a single block it is arithmetic instead, and it is derived
      // where it is needed rather than assumed everywhere.
      predicate normfs_wal_ring_layout{L}(struct normfs_wal_ring *r) =
        \valid(r->arena + (0 .. r->page_count * r->page_size - 1)) &&
        (\forall integer k; 0 <= k < r->page_count ==>
           r->pages[k].buf == r->arena + k * r->page_size);

      // The ring, the descriptor array and the arena are three disjoint
      // regions. No quantifier: every term is a base pointer or a scalar, and
      // no function assigns any of them, so this survives a mutation by frame.
      predicate normfs_wal_ring_sep{L}(struct normfs_wal_ring *r) =
        \separated(r, r->pages + (0 .. r->page_count - 1)) &&
        \separated(r, r->arena + (0 .. r->page_count * r->page_size - 1)) &&
        \separated(r->pages + (0 .. r->page_count - 1),
                   r->arena + (0 .. r->page_count * r->page_size - 1));

      predicate normfs_wal_ring_wf{L}(struct normfs_wal_ring *r) =
        \valid(r) &&
        \valid(r->pages + (0 .. r->page_count - 1)) &&
        normfs_wal_ring_scalar_wf(r) &&
        normfs_wal_ring_layout(r) &&
        normfs_wal_ring_sep(r) &&
        normfs_wal_ring_pages_wf(r);
    }
*/

/*
 * Durability
 * ==========
 *
 * The property this ring exists to guarantee: **a record the ring accepted is
 * not overwritten in memory before it is on disk.**
 *
 * The ring cannot check that by itself -- it has no idea what a file is, and
 * that is on purpose. What it does instead is refuse to reuse a page until it
 * is told the page's records are safe, and the telling is `min_essential_id`.
 * The caller's obligation, and the other half of the theorem, is that
 * `normfs_wal_ring_set_essential(r, m)` is called only once every entry with id
 * `< m` has been written and synced. In this tree that call has exactly one
 * source: the WAL file writer, after `fsync` returns.
 *
 * Given that obligation, the C side of the theorem is already discharged, and
 * `normfs_wal_ring_reuse_is_safe` below is its statement. Reuse happens in one
 * place -- `normfs_wal_ring_rotate_to` -- and its `requires` is exactly this
 * predicate. So there is no path through this module that overwrites a page
 * holding a record below the durable watermark, and none that overwrites a page
 * a reader is holding.
 *
 * The two conjuncts answer two different questions, and both must hold:
 *
 *   pin_count == 0        nobody is reading these bytes right now
 *   last_entry_id < m     these bytes are on disk
 *
 * Dropping either one loses records: without the first, a stream reads a page
 * out from under itself; without the second, an accepted record is gone before
 * it was ever written.
 */
/*@ axiomatic NormfsWalRingDurability {
      // Everything this page holds has been reported durable. An empty page
      // trivially qualifies -- it holds nothing to lose.
      predicate normfs_wal_page_all_durable{L}(struct normfs_wal_page *p,
                                               integer m) =
        p->count == 0 || p->last_entry_id < m;

      // Page k may be handed back to be written over.
      predicate normfs_wal_ring_reuse_is_safe{L}(struct normfs_wal_ring *r,
                                                 integer k) =
        0 <= k < r->page_count &&
        r->pages[k].pin_count == 0 &&
        normfs_wal_page_all_durable(&r->pages[k], r->min_essential_id);
    }
*/

/*@ requires \valid(ring);
    requires page_count >= 1;
    requires page_size >= NORMFS_WAL_ENTRY_V1_MIN_SIZE + 4;
    requires page_size <= 0xFFFFFFFF;
    requires \valid(pages + (0 .. page_count - 1));
    requires \forall integer k; 0 <= k < page_count ==>
               normfs_wal_page_wf(&pages[k]) && pages[k].cap == page_size &&
               pages[k].count == 0;
    requires pages[0].first_entry_id == first_entry_id;
    // The caller lays the pages out over one allocation. What used to be a
    // quantifier over every pair of page buffers is now this one equation.
    requires page_count * page_size <= 0xFFFFFFFFFFFFFFFF;
    requires \valid(arena + (0 .. page_count * page_size - 1));
    requires \forall integer k; 0 <= k < page_count ==>
               pages[k].buf == arena + k * page_size;
    requires \separated(ring, pages + (0 .. page_count - 1));
    requires \separated(ring, arena + (0 .. page_count * page_size - 1));
    requires \separated(pages + (0 .. page_count - 1),
                        arena + (0 .. page_count * page_size - 1));
    assigns ring->pages, ring->arena, ring->page_count, ring->page_size,
            ring->active, ring->next_entry_id, ring->next_page_id,
            ring->min_essential_id;
    ensures normfs_wal_ring_wf(ring);
    ensures ring->active == 0 && ring->next_entry_id == first_entry_id;
*/
void normfs_wal_ring_init(struct normfs_wal_ring *ring,
    struct normfs_wal_page *pages, uint8_t *arena, size_t page_count,
    size_t page_size, uint64_t first_entry_id);

/* This contract is the intended specification, but try_append (like
 * rotate_to) is verified by the WalRing Rust tests rather than WP:
 * re-establishing every page's page_wf after a mutation is a nested-quantifier
 * frame over the offset tables that the automatic provers do not discharge.
 * Its callee page_append is fully proven. */
/*@ requires normfs_wal_ring_wf(ring);
    requires ring->next_entry_id < 0xFFFFFFFFFFFFFFFF;
    requires record_size == 0 || \valid_read(record + (0 .. record_size - 1));
    requires record_size == 0 ||
             \separated(ring->pages[ring->active].buf +
                          (0 .. ring->page_size - 1),
                        record + (0 .. record_size - 1));
    requires \separated(ring, ring->pages[ring->active].buf +
                                (0 .. ring->page_size - 1));
    assigns ring->next_entry_id,
            ring->pages[ring->active].used_bytes,
            ring->pages[ring->active].count,
            ring->pages[ring->active].last_entry_id,
            ring->pages[ring->active].buf[0 .. ring->page_size - 1];
    ensures normfs_wal_ring_wf(ring);
    ensures \result.status == NORMFS_WAL_RING_OK ||
            \result.status == NORMFS_WAL_RING_NEEDS_ROTATE ||
            \result.status == NORMFS_WAL_RING_ERR_TOO_LARGE;
    ensures \result.status == NORMFS_WAL_RING_OK ==>
            \result.entry_id == \old(ring->next_entry_id) &&
            ring->next_entry_id == \old(ring->next_entry_id) + 1 &&
            \result.page_index == ring->active;
    ensures \result.status == NORMFS_WAL_RING_ERR_TOO_LARGE ==>
            normfs_wal_entry_v1_size_logic(record_size) + 4 > ring->page_size;
    ensures \result.status != NORMFS_WAL_RING_OK ==>
            ring->next_entry_id == \old(ring->next_entry_id) &&
            ring->active == \old(ring->active);
*/
struct normfs_wal_ring_append_result
normfs_wal_ring_try_append(struct normfs_wal_ring *ring, const uint8_t *record,
    uint32_t record_size);

/*@ requires normfs_wal_ring_wf(ring);
    assigns \nothing;
    ensures \result.found != 0 ==>
            \result.index < ring->page_count &&
            normfs_wal_page_is_reusable(&ring->pages[\result.index],
                                        ring->min_essential_id);
*/
struct normfs_wal_ring_reusable_result
normfs_wal_ring_find_reusable(struct normfs_wal_ring *ring);

/* This is the one place a page's contents are discarded, which makes it the
 * one place the durability theorem is used -- see the `requires` and the
 * page-intactness `ensures` below.
 *
 * It is WP-verified. That took two things. Its only callee, page_reset,
 * assigns five scalar fields and no buffer bytes at all, so no page's offset
 * table is in its footprint and the frame is precise; the asserts in the body
 * are what let WP split ring_wf's quantifier over pages into the reset page
 * and the rest. And separation had to stop being quantified: while each page
 * owned an independent buffer, re-establishing ring_sep meant transporting a
 * \forall over every *pair* of buffers across the call, which the automatic
 * provers would not do. Laying the pages over one arena made that arithmetic
 * instead (see normfs_wal_ring_layout), and it was the last goal to fall. */
/*@ requires normfs_wal_ring_wf(ring);
    requires index < ring->page_count;
    // The durability theorem, at its one point of use: this is the only place
    // a page's contents are discarded, and it may not happen while a reader
    // holds the page or while it holds a record that is not yet on disk.
    requires normfs_wal_ring_reuse_is_safe(ring, index);
    requires normfs_wal_page_is_reusable(&ring->pages[index],
                                         ring->min_essential_id);
    assigns ring->active, ring->next_page_id,
            ring->pages[index].used_bytes,
            ring->pages[index].count,
            ring->pages[index].page_id,
            ring->pages[index].first_entry_id,
            ring->pages[index].last_entry_id;
    // normfs_wal_ring_wf, spelled out one conjunct at a time. The conjunction
    // is what callers consume, but as a single clause an unproved goal says
    // only "well-formedness was not re-established" -- naming the parts costs
    // nothing and says which part.
    ensures \valid(ring);
    ensures \valid(ring->pages + (0 .. ring->page_count - 1));
    ensures normfs_wal_ring_scalar_wf(ring);
    ensures normfs_wal_ring_sep(ring);
    ensures normfs_wal_ring_pages_wf(ring);
    ensures ring->active == index && ring->pages[index].count == 0;
    ensures ring->pages[index].first_entry_id == \old(ring->next_entry_id);
    ensures ring->next_entry_id == \old(ring->next_entry_id);

    // The durability theorem as an obligation rather than a decoration.
    //
    // A `requires` on an entry point is an assumption: WP proves the body
    // under it and no C caller exists to discharge it, so on its own it is a
    // comment with a syntax checker. This clause is what makes it carry
    // weight -- it says every page that was *not* reusable before the call is
    // byte-for-byte unchanged after it. At k != index that follows from the
    // frame. At k == index the conclusion is false, because that page was
    // just reset, so the only way to discharge the implication is to refute
    // its antecedent -- which is exactly normfs_wal_ring_reuse_is_safe above.
    //
    // Delete that `requires` and this goal must go red. That is the check
    // that says the theorem is doing something.
    ensures \forall integer k; 0 <= k < ring->page_count ==>
              ( \at(ring->pages[k].pin_count, Pre) != 0 ||
                ( \at(ring->pages[k].count, Pre) != 0 &&
                  \at(ring->pages[k].last_entry_id, Pre) >=
                    \at(ring->min_essential_id, Pre) ) )
              ==>
              ( ring->pages[k].used_bytes ==
                  \at(ring->pages[k].used_bytes, Pre) &&
                ring->pages[k].count == \at(ring->pages[k].count, Pre) &&
                ring->pages[k].first_entry_id ==
                  \at(ring->pages[k].first_entry_id, Pre) &&
                ring->pages[k].last_entry_id ==
                  \at(ring->pages[k].last_entry_id, Pre) );

    // Free from the frame, and states the other half: rotation never drops a
    // reader's pin.
    ensures \forall integer k; 0 <= k < ring->page_count ==>
              ring->pages[k].pin_count == \at(ring->pages[k].pin_count, Pre);
*/
void normfs_wal_ring_rotate_to(struct normfs_wal_ring *ring, size_t index);

/*@ requires normfs_wal_ring_wf(ring);
    assigns \nothing;
    ensures \result.found != 0 ==>
            \result.page_index < ring->page_count &&
            \result.index < ring->pages[\result.page_index].count &&
            ring->pages[\result.page_index].first_entry_id +
              (integer)\result.index == entry_id;
*/
struct normfs_wal_ring_seek_result
normfs_wal_ring_seek(struct normfs_wal_ring *ring, uint64_t entry_id);

/*@ requires \valid(ring);
    assigns ring->min_essential_id;
    ensures ring->min_essential_id == min_essential_id;
*/
void normfs_wal_ring_set_essential(struct normfs_wal_ring *ring,
    uint64_t min_essential_id);

#endif /* NORMFS_WAL_RING_H */
