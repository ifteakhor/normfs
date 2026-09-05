#include "normfs/wal_pool.h"

void
normfs_wal_pool_init(struct normfs_wal_pool *pool, struct normfs_wal_page *pages,
    uint8_t *arena, uint64_t *owner, size_t page_count, size_t page_size)
{
	pool->pages = pages;
	pool->arena = arena;
	pool->owner = owner;
	pool->page_count = page_count;
	pool->page_size = page_size;

	/*@ assert scalars_final:
	      pool->page_count >= 1 &&
	      pool->page_size >= NORMFS_WAL_ENTRY_V1_MIN_SIZE + 4 &&
	      pool->page_size <= 0xFFFFFFFF; */
	/*@ assert arrays_valid_final:
	      \valid(pool->pages + (0 .. pool->page_count - 1)) &&
	      \valid(pool->owner + (0 .. pool->page_count - 1)); */
	/*@ assert pages_wf_final:
	      \forall integer k; 0 <= k < pool->page_count ==>
	        normfs_wal_page_wf(&pool->pages[k]) &&
	        pool->pages[k].cap == pool->page_size; */

	/* The parts come straight from the preconditions; the fold into the
	 * predicate is the step left over, and it is cheaper here than at the
	 * ensures, one statement further from where they were established. */
	/*@ assert layout_final: normfs_wal_pool_layout(pool); */
	/*@ assert sep_final: normfs_wal_pool_sep(pool); */
	/*@ assert wf_final: normfs_wal_pool_wf(pool); */
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
	/* pool_wf reaches the prover as an opaque atom: the separations stated
	 * inside it are not hypotheses until an assertion asks for one, and every
	 * fact needed to re-establish it after the write below is one of them. */
	/*@ assert unfold_pool_vs_owner:
	      \separated(pool, pool->owner + (0 .. pool->page_count - 1)); */
	/*@ assert unfold_pool_vs_pages:
	      \separated(pool, pool->pages + (0 .. pool->page_count - 1)); */
	/*@ assert unfold_pool_vs_arena:
	      \separated(pool, pool->arena +
	                   (0 .. pool->page_count * pool->page_size - 1)); */
	/*@ assert unfold_owner_vs_pages:
	      \separated(pool->owner + (0 .. pool->page_count - 1),
	                 pool->pages + (0 .. pool->page_count - 1)); */
	/*@ assert unfold_owner_vs_arena:
	      \separated(pool->owner + (0 .. pool->page_count - 1),
	                 pool->arena +
	                   (0 .. pool->page_count * pool->page_size - 1)); */
	/*@ assert unfold_pages_vs_arena:
	      \separated(pool->pages + (0 .. pool->page_count - 1),
	                 pool->arena +
	                   (0 .. pool->page_count * pool->page_size - 1)); */

	/* The write lands on one element of the owner array, and narrowing a
	 * separation to a single element is a step WP does not take on its own. */
	/*@ assert slot_vs_pool: \separated(&pool->owner[index], pool); */
	/*@ assert slot_vs_pages:
	      \separated(&pool->owner[index],
	                 pool->pages + (0 .. pool->page_count - 1)); */
	/*@ assert slot_vs_arena:
	      \separated(&pool->owner[index],
	                 pool->arena +
	                   (0 .. pool->page_count * pool->page_size - 1)); */
	/*@ assert slot_vs_each_page:
	      \forall integer k; 0 <= k < pool->page_count ==>
	        \separated(&pool->owner[index], &pool->pages[k]); */
	/* Page by page, then at the four bytes offsets_wf reads per entry, which
	 * is the form the frame uses: given only the page-wide range, WP has to
	 * place each read inside it while already under offsets_wf's own
	 * quantifier over entries. */
	/* Page k's bytes are [k * page_size, (k + 1) * page_size), and the
	 * separation above is against the whole arena, so placing one page inside
	 * it is monotonicity of multiplication on the left factor -- nonlinear,
	 * and not derived from the inequality on its own. */
	/*@ assert page_bytes_inside_arena:
	      \forall integer k; 0 <= k < pool->page_count ==>
	        (k + 1) * pool->page_size <=
	          pool->page_count * pool->page_size; */
	/*@ assert slot_vs_each_page_bytes:
	      \forall integer k; 0 <= k < pool->page_count ==>
	        \separated(&pool->owner[index],
	                   pool->pages[k].buf + (0 .. pool->page_size - 1)); */
	/*@ assert slot_vs_tables:
	      \forall integer k, i;
	        0 <= k < pool->page_count && 0 <= i < pool->pages[k].count ==>
	          \separated(&pool->owner[index],
	                     pool->pages[k].buf + pool->pages[k].cap - 4 * (i + 1)
	                       + (0 .. 3)); */

	pool->owner[index] = ring_id;

	/* The frame's conclusion as an equality between the two states, which the
	 * separations above feed directly; page_wf then follows by rewriting
	 * rather than by being reproved. */
	/*@ assert offsets_unchanged:
	      \forall integer k, i;
	        0 <= k < pool->page_count && 0 <= i < pool->pages[k].count ==>
	          normfs_wal_page_offset_logic(&pool->pages[k], i) ==
	            \at(normfs_wal_page_offset_logic(&pool->pages[k], i), Pre); */
	/*@ assert pages_intact:
	      \forall integer k; 0 <= k < pool->page_count ==>
	        normfs_wal_page_wf(&pool->pages[k]) &&
	        pool->pages[k].cap == pool->page_size; */
	/*@ assert layout_final: normfs_wal_pool_layout(pool); */
	/*@ assert sep_final: normfs_wal_pool_sep(pool); */
	/*@ assert wf_final: normfs_wal_pool_wf(pool); */
}

void
normfs_wal_pool_give_back(struct normfs_wal_pool *pool, size_t index,
    uint64_t ring_id, uint64_t min_essential_id)
{
	(void)ring_id;
	(void)min_essential_id;

	/* pool_wf reaches the prover as an opaque atom: the separations stated
	 * inside it are not hypotheses until an assertion asks for one, and every
	 * fact needed to re-establish it after the write below is one of them. */
	/*@ assert unfold_pool_vs_owner:
	      \separated(pool, pool->owner + (0 .. pool->page_count - 1)); */
	/*@ assert unfold_pool_vs_pages:
	      \separated(pool, pool->pages + (0 .. pool->page_count - 1)); */
	/*@ assert unfold_pool_vs_arena:
	      \separated(pool, pool->arena +
	                   (0 .. pool->page_count * pool->page_size - 1)); */
	/*@ assert unfold_owner_vs_pages:
	      \separated(pool->owner + (0 .. pool->page_count - 1),
	                 pool->pages + (0 .. pool->page_count - 1)); */
	/*@ assert unfold_owner_vs_arena:
	      \separated(pool->owner + (0 .. pool->page_count - 1),
	                 pool->arena +
	                   (0 .. pool->page_count * pool->page_size - 1)); */
	/*@ assert unfold_pages_vs_arena:
	      \separated(pool->pages + (0 .. pool->page_count - 1),
	                 pool->arena +
	                   (0 .. pool->page_count * pool->page_size - 1)); */

	/* The write lands on one element of the owner array, and narrowing a
	 * separation to a single element is a step WP does not take on its own. */
	/*@ assert slot_vs_pool: \separated(&pool->owner[index], pool); */
	/*@ assert slot_vs_pages:
	      \separated(&pool->owner[index],
	                 pool->pages + (0 .. pool->page_count - 1)); */
	/*@ assert slot_vs_arena:
	      \separated(&pool->owner[index],
	                 pool->arena +
	                   (0 .. pool->page_count * pool->page_size - 1)); */
	/*@ assert slot_vs_each_page:
	      \forall integer k; 0 <= k < pool->page_count ==>
	        \separated(&pool->owner[index], &pool->pages[k]); */
	/* Page by page, then at the four bytes offsets_wf reads per entry, which
	 * is the form the frame uses: given only the page-wide range, WP has to
	 * place each read inside it while already under offsets_wf's own
	 * quantifier over entries. */
	/* Page k's bytes are [k * page_size, (k + 1) * page_size), and the
	 * separation above is against the whole arena, so placing one page inside
	 * it is monotonicity of multiplication on the left factor -- nonlinear,
	 * and not derived from the inequality on its own. */
	/*@ assert page_bytes_inside_arena:
	      \forall integer k; 0 <= k < pool->page_count ==>
	        (k + 1) * pool->page_size <=
	          pool->page_count * pool->page_size; */
	/*@ assert slot_vs_each_page_bytes:
	      \forall integer k; 0 <= k < pool->page_count ==>
	        \separated(&pool->owner[index],
	                   pool->pages[k].buf + (0 .. pool->page_size - 1)); */
	/*@ assert slot_vs_tables:
	      \forall integer k, i;
	        0 <= k < pool->page_count && 0 <= i < pool->pages[k].count ==>
	          \separated(&pool->owner[index],
	                     pool->pages[k].buf + pool->pages[k].cap - 4 * (i + 1)
	                       + (0 .. 3)); */

	pool->owner[index] = NORMFS_WAL_POOL_FREE;

	/* The frame's conclusion as an equality between the two states, which the
	 * separations above feed directly; page_wf then follows by rewriting
	 * rather than by being reproved. */
	/*@ assert offsets_unchanged:
	      \forall integer k, i;
	        0 <= k < pool->page_count && 0 <= i < pool->pages[k].count ==>
	          normfs_wal_page_offset_logic(&pool->pages[k], i) ==
	            \at(normfs_wal_page_offset_logic(&pool->pages[k], i), Pre); */
	/*@ assert pages_intact:
	      \forall integer k; 0 <= k < pool->page_count ==>
	        normfs_wal_page_wf(&pool->pages[k]) &&
	        pool->pages[k].cap == pool->page_size; */
	/*@ assert layout_final: normfs_wal_pool_layout(pool); */
	/*@ assert sep_final: normfs_wal_pool_sep(pool); */
	/*@ assert wf_final: normfs_wal_pool_wf(pool); */
}
