#ifndef NORMFS_WAL_POOL_H
#define NORMFS_WAL_POOL_H

#include <stddef.h>
#include <stdint.h>

#include "normfs/wal_page.h"

/*
 * One arena of WAL pages, shared by every queue.
 *
 * Rust allocates the arena once, at startup, and hands it here; C never
 * allocates. Page k is the arena slice at k * page_size, so distinct pages are
 * disjoint by arithmetic -- the same reason the ring's own separation clause
 * is three flat statements rather than a quantifier over pairs.
 *
 * A page belongs to at most one ring at a time, and `owner` is what says so:
 * each page is either NORMFS_WAL_POOL_FREE or carries the id of the ring
 * holding it. That is the whole of cross-queue exclusivity, and it is
 * exclusive by construction -- there is one slot per page to write an owner
 * into, so "two rings hold the same page" is not a state this can represent.
 *
 * Ownership moves in two operations and the durability theorem lives on one of
 * them: a page becomes free only once nothing pins it and everything on it is
 * on disk, and only a free page is ever handed out to be written over. Between
 * those two facts, no queue can be given a page whose records another queue
 * has not yet got to disk.
 */

#define NORMFS_WAL_POOL_FREE 0xFFFFFFFFFFFFFFFFu

struct normfs_wal_pool {
	struct normfs_wal_page *pages;
	uint8_t *arena;
	uint64_t *owner;
	size_t page_count;
	size_t page_size;
};

struct normfs_wal_pool_take_result {
	size_t index;
	int found;
};

/*@ axiomatic NormfsWalPool {
      // Every page is a slice of the one arena, in index order. Stated once
      // here rather than once per ring, which is the point of the pool.
      predicate normfs_wal_pool_layout{L}(struct normfs_wal_pool *p) =
        \valid(p->arena + (0 .. p->page_count * p->page_size - 1)) &&
        (\forall integer k; 0 <= k < p->page_count ==>
           p->pages[k].buf == p->arena + k * p->page_size);

      // Four disjoint regions: the pool, the descriptor array, the owner
      // array and the arena. No quantifier -- every term is a base pointer,
      // and nothing assigns any of them, so this survives a mutation by frame.
      predicate normfs_wal_pool_sep{L}(struct normfs_wal_pool *p) =
        \separated(p, p->pages + (0 .. p->page_count - 1)) &&
        \separated(p, p->owner + (0 .. p->page_count - 1)) &&
        \separated(p, p->arena + (0 .. p->page_count * p->page_size - 1)) &&
        \separated(p->pages + (0 .. p->page_count - 1),
                   p->owner + (0 .. p->page_count - 1)) &&
        \separated(p->pages + (0 .. p->page_count - 1),
                   p->arena + (0 .. p->page_count * p->page_size - 1)) &&
        \separated(p->owner + (0 .. p->page_count - 1),
                   p->arena + (0 .. p->page_count * p->page_size - 1));

      predicate normfs_wal_pool_wf{L}(struct normfs_wal_pool *p) =
        \valid(p) &&
        p->page_count >= 1 &&
        p->page_size >= NORMFS_WAL_ENTRY_V1_MIN_SIZE + 4 &&
        p->page_size <= 0xFFFFFFFF &&
        \valid(p->pages + (0 .. p->page_count - 1)) &&
        \valid(p->owner + (0 .. p->page_count - 1)) &&
        (\forall integer k; 0 <= k < p->page_count ==>
           normfs_wal_page_wf(&p->pages[k]) && p->pages[k].cap == p->page_size) &&
        normfs_wal_pool_layout(p) &&
        normfs_wal_pool_sep(p);

      predicate normfs_wal_pool_owns{L}(struct normfs_wal_pool *p, integer k,
                                        uint64_t ring_id) =
        0 <= k < p->page_count && p->owner[k] == ring_id;
    }
*/

/*@ requires \valid(pool);
    requires page_count >= 1;
    requires page_size >= NORMFS_WAL_ENTRY_V1_MIN_SIZE + 4;
    requires page_size <= 0xFFFFFFFF;
    requires page_count * page_size <= 0xFFFFFFFFFFFFFFFF;
    requires \valid(pages + (0 .. page_count - 1));
    requires \valid(owner + (0 .. page_count - 1));
    requires \valid(arena + (0 .. page_count * page_size - 1));
    requires \forall integer k; 0 <= k < page_count ==>
               normfs_wal_page_wf(&pages[k]) && pages[k].cap == page_size &&
               pages[k].buf == arena + k * page_size;
    requires \separated(pool, pages + (0 .. page_count - 1));
    requires \separated(pool, owner + (0 .. page_count - 1));
    requires \separated(pool, arena + (0 .. page_count * page_size - 1));
    requires \separated(pages + (0 .. page_count - 1),
                        owner + (0 .. page_count - 1));
    requires \separated(pages + (0 .. page_count - 1),
                        arena + (0 .. page_count * page_size - 1));
    requires \separated(owner + (0 .. page_count - 1),
                        arena + (0 .. page_count * page_size - 1));
    // The caller marks the pages free, as it already initialises each page
    // descriptor. C initialising what the caller can is what forces this
    // function to write two disjoint regions and then re-establish a
    // quantified property across both -- for no benefit, since Rust allocates
    // the owner array and can fill it as it allocates.
    requires \forall integer k; 0 <= k < page_count ==>
               owner[k] == NORMFS_WAL_POOL_FREE;
    assigns pool->pages, pool->arena, pool->owner, pool->page_count,
            pool->page_size;
    ensures normfs_wal_pool_wf(pool);
    ensures \forall integer k; 0 <= k < page_count ==>
              pool->owner[k] == NORMFS_WAL_POOL_FREE;
*/
void normfs_wal_pool_init(struct normfs_wal_pool *pool,
    struct normfs_wal_page *pages, uint8_t *arena, uint64_t *owner,
    size_t page_count, size_t page_size);

/*@ requires normfs_wal_pool_wf(pool);
    requires ring_id != NORMFS_WAL_POOL_FREE;
    assigns \nothing;
    ensures \result.found != 0 ==>
            \result.index < pool->page_count &&
            pool->owner[\result.index] == NORMFS_WAL_POOL_FREE;
*/
struct normfs_wal_pool_take_result
normfs_wal_pool_find_free(struct normfs_wal_pool *pool, uint64_t ring_id);

/*@ requires normfs_wal_pool_wf(pool);
    requires index < pool->page_count;
    requires ring_id != NORMFS_WAL_POOL_FREE;
    requires pool->owner[index] == NORMFS_WAL_POOL_FREE;
    assigns pool->owner[index];
    ensures normfs_wal_pool_wf(pool);
    ensures normfs_wal_pool_owns(pool, index, ring_id);
    ensures \forall integer k; 0 <= k < pool->page_count && k != index ==>
              pool->owner[k] == \at(pool->owner[k], Pre);
*/
void normfs_wal_pool_take(struct normfs_wal_pool *pool, size_t index,
    uint64_t ring_id);

/*@ requires normfs_wal_pool_wf(pool);
    requires index < pool->page_count;
    requires ring_id != NORMFS_WAL_POOL_FREE;
    requires normfs_wal_pool_owns(pool, index, ring_id);
    // The durability theorem, at the point ownership is released.
    //
    // A page can only be handed to another queue once it is free, and this is
    // the only way to become free. So a page carrying records that are not yet
    // on disk, or that a reader still holds, cannot cross to another queue --
    // and rotate_to, which is where contents are actually discarded, requires
    // the same predicate again at its own point of use.
    requires normfs_wal_page_is_reusable(&pool->pages[index], min_essential_id);
    assigns pool->owner[index];
    ensures normfs_wal_pool_wf(pool);
    ensures pool->owner[index] == NORMFS_WAL_POOL_FREE;
    ensures \forall integer k; 0 <= k < pool->page_count && k != index ==>
              pool->owner[k] == \at(pool->owner[k], Pre);
    // Giving a page back does not touch what is on it: the bytes stay put
    // until whoever takes it next resets it.
    ensures \forall integer k; 0 <= k < pool->page_count ==>
              pool->pages[k].used_bytes == \at(pool->pages[k].used_bytes, Pre) &&
              pool->pages[k].count == \at(pool->pages[k].count, Pre) &&
              pool->pages[k].pin_count == \at(pool->pages[k].pin_count, Pre);
*/
void normfs_wal_pool_give_back(struct normfs_wal_pool *pool, size_t index,
    uint64_t ring_id, uint64_t min_essential_id);

#endif /* NORMFS_WAL_POOL_H */
