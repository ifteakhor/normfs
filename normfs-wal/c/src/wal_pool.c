#include "normfs/wal_pool.h"

void
normfs_wal_pool_init(struct normfs_wal_pool *pool, struct normfs_wal_page *pages,
    uint8_t *arena, uint64_t *owner, size_t page_count, size_t page_size)
{
	size_t k;

	pool->pages = pages;
	pool->arena = arena;
	pool->owner = owner;
	pool->page_count = page_count;
	pool->page_size = page_size;

	/*@ loop invariant 0 <= k <= page_count;
	    loop invariant \forall integer i; 0 <= i < k ==>
	                     owner[i] == NORMFS_WAL_POOL_FREE;
	    loop assigns k, owner[0 .. page_count - 1];
	    loop variant page_count - k;
	*/
	for (k = 0u; k < page_count; k++) {
		owner[k] = NORMFS_WAL_POOL_FREE;
	}
}

struct normfs_wal_pool_take_result
normfs_wal_pool_find_free(struct normfs_wal_pool *pool, uint64_t ring_id)
{
	struct normfs_wal_pool_take_result r;
	size_t k;

	(void)ring_id;

	r.index = 0u;
	r.found = 0;

	/*@ loop invariant 0 <= k <= pool->page_count;
	    loop invariant r.found == 0;
	    loop assigns k, r;
	    loop variant pool->page_count - k;
	*/
	for (k = 0u; k < pool->page_count; k++) {
		if (pool->owner[k] == NORMFS_WAL_POOL_FREE) {
			r.index = k;
			r.found = 1;
			return r;
		}
	}

	return r;
}

void
normfs_wal_pool_take(struct normfs_wal_pool *pool, size_t index, uint64_t ring_id)
{
	pool->owner[index] = ring_id;
}

void
normfs_wal_pool_give_back(struct normfs_wal_pool *pool, size_t index,
    uint64_t ring_id, uint64_t min_essential_id)
{
	(void)ring_id;
	(void)min_essential_id;

	pool->owner[index] = NORMFS_WAL_POOL_FREE;
}
