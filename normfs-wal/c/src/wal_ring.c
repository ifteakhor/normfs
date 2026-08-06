#include "normfs/wal_ring.h"

void
normfs_wal_ring_init(struct normfs_wal_ring *ring, struct normfs_wal_page *pages,
    uint8_t *arena, size_t page_count, size_t page_size, uint64_t first_entry_id)
{
	/* Unbound: this ring owns its pages outright. Binding it to a pool is a
	 * separate step, so a ring built for a standalone test needs no pool. */
	ring->pool = NULL;
	ring->first_slot = 0u;
	ring->pages = pages;
	ring->arena = arena;
	ring->page_count = page_count;
	ring->page_size = page_size;
	ring->active = 0u;
	ring->next_entry_id = first_entry_id;
	ring->next_page_id = page_count;
	ring->min_essential_id = 0u;
}

void
normfs_wal_ring_set_essential(struct normfs_wal_ring *ring,
    uint64_t min_essential_id)
{
	ring->min_essential_id = min_essential_id;
}

void
normfs_wal_ring_grow(struct normfs_wal_ring *ring, uint64_t ring_id)
{
	/* The range grows by a page, so the ring's view of the arena has to
	 * stretch with it. That the bytes are there is the pool's -- the ring's
	 * arena is the pool's at first_slot, and the slot taken is inside the
	 * pool -- but the arithmetic joining the two is not automatic. The step
	 * that is not is the second: multiplying an inequality by page_size.
	 * Left implicit, the whole chain fails as one goal with nothing to say
	 * about which link broke. */
	/*@ assert slots_fit:
	      ring->first_slot + ring->page_count + 1 <= ring->pool->page_count; */
	/*@ assert bytes_fit:
	      (ring->first_slot + ring->page_count + 1) * ring->page_size <=
	        ring->pool->page_count * ring->page_size; */
	/*@ assert arena_covers_new_page:
	      \valid(ring->arena +
	             (0 .. (ring->page_count + 1) * ring->page_size - 1)); */

	normfs_wal_pool_take(ring->pool, ring->first_slot + ring->page_count,
	    ring_id);

	/* Splits ring_wf's \forall over pages into the page just taken and the
	 * rest, which WP does not split on its own -- the same shape rotate_to
	 * needs, for the same reason. Stated here, next to the call whose assigns
	 * clause proves the second half: pool_take writes one owner slot, and the
	 * pool's own separation puts the owner array outside both the descriptor
	 * array and the arena, so no page moved and no page's bytes did. */
	/*@ assert old_pages_intact:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        normfs_wal_page_wf(&ring->pages[k]) &&
	        ring->pages[k].cap == ring->page_size &&
	        ring->pages[k].buf == ring->arena + k * ring->page_size; */
	/*@ assert new_page_wf:
	      normfs_wal_page_wf(&ring->pages[ring->page_count]); */

	/* normfs_wal_ring_sep at the width the range is about to become. Each
	 * comes from in_pool, whose separation is stated over the whole pool and
	 * so does not shrink to the range the ring holds today. */
	/*@ assert sep_ring_arena:
	      \separated(ring, ring->arena +
	                   (0 .. (ring->page_count + 1) * ring->page_size - 1)); */
	/*@ assert sep_ring_pages:
	      \separated(ring, ring->pages + (0 .. ring->page_count)); */
	/*@ assert sep_pages_arena:
	      \separated(ring->pages + (0 .. ring->page_count),
	                 ring->arena +
	                   (0 .. (ring->page_count + 1) * ring->page_size - 1)); */

	ring->page_count = ring->page_count + 1u;
}

void
normfs_wal_ring_shrink(struct normfs_wal_ring *ring, uint64_t ring_id)
{
	/* The count comes down first, so from here `page_count` names the page
	 * being released and the range below it is already the range that
	 * survives. Nothing about the pages changed: the only write so far is to
	 * the ring's own storage, which in_pool puts outside the descriptor
	 * array -- without that clause not even this would carry. */
	ring->page_count = ring->page_count - 1u;

	/* The page leaving is the pool's slot first_slot + page_count. Since
	 * ring->pages is based at first_slot, the ring's index and the pool's
	 * name one descriptor, and it is the page the durability precondition
	 * spoke about. */
	/*@ assert same_page:
	      &ring->pages[ring->page_count] ==
	        &ring->pool->pages[ring->first_slot + ring->page_count]; */
	/*@ assert still_reusable:
	      normfs_wal_page_is_reusable(&ring->pages[ring->page_count],
	                                  ring->min_essential_id); */

	normfs_wal_pool_give_back(ring->pool, ring->first_slot + ring->page_count,
	    ring_id, ring->min_essential_id);

	/* give_back writes one owner slot and says every other one is untouched,
	 * so the range that survives still reads as this ring's.
	 *
	 * Twice, and the order is the point. give_back states preservation over
	 * the pool's own index space -- a flat range of j -- while in_pool wants
	 * it over the ring's, at the shifted index first_slot + k. Asking for the
	 * shifted form directly means instantiating the callee's quantifier under
	 * a shift, which WP does not do. Stated first in the shape give_back
	 * already speaks, it is one instantiation each way. */
	/*@ assert slots_in_pool:
	      ring->first_slot + ring->page_count < ring->pool->page_count; */
	/*@ assert owners_absolute:
	      \forall integer j;
	        ring->first_slot <= j < ring->first_slot + ring->page_count ==>
	        ring->pool->owner[j] == ring_id; */
	/*@ assert owners_below_intact:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        ring->pool->owner[ring->first_slot + k] == ring_id; */
}

struct normfs_wal_ring_append_result
normfs_wal_ring_try_append(struct normfs_wal_ring *ring, const uint8_t *record,
    uint32_t record_size)
{
	struct normfs_wal_ring_append_result r;
	struct normfs_wal_page_append_result ap;
	size_t entry_size;

	r.entry_id = 0u;
	r.page_index = 0u;

	entry_size = normfs_wal_entry_v1_size(record_size);
	if (entry_size + 4u > ring->page_size) {
		r.status = NORMFS_WAL_RING_ERR_TOO_LARGE;
		return r;
	}

	ap = normfs_wal_page_append(&ring->pages[ring->active], record,
	    record_size);
	if (ap.status == NORMFS_WAL_PAGE_OK) {
		r.entry_id = ring->next_entry_id;
		r.page_index = ring->active;
		ring->next_entry_id = ring->next_entry_id + 1u;
		r.status = NORMFS_WAL_RING_OK;
		return r;
	}

	r.status = NORMFS_WAL_RING_NEEDS_ROTATE;
	return r;
}

struct normfs_wal_ring_reusable_result
normfs_wal_ring_find_reusable(struct normfs_wal_ring *ring)
{
	struct normfs_wal_ring_reusable_result r;
	size_t k;

	r.index = 0u;
	r.found = 0;

	/*@ loop invariant 0 <= k <= ring->page_count;
	    loop invariant r.found == 0;
	    loop assigns k, r;
	    loop variant ring->page_count - k;
	*/
	for (k = 0u; k < ring->page_count; k++) {
		if (normfs_wal_page_reusable(&ring->pages[k],
		    ring->min_essential_id) != 0) {
			r.index = k;
			r.found = 1;
			return r;
		}
	}

	return r;
}

void
normfs_wal_ring_rotate_to(struct normfs_wal_ring *ring, size_t index)
{
	normfs_wal_page_reset(&ring->pages[index], ring->next_page_id,
	    ring->next_entry_id);

	/* Splits the \forall over pages into the two cases WP does not separate
	 * on its own. Stated here, next to the call whose assigns clause proves
	 * it, it costs seconds; deferred to the end of the body it costs minutes.
	 * It stays a hypothesis for the rest of the body, so the whole-ring
	 * statement below follows cheaply. */
	/*@ assert other_pages_intact:
	      \forall integer k; 0 <= k < ring->page_count && k != index ==>
	        normfs_wal_page_wf(&ring->pages[k]); */
	/*@ assert reset_page_wf: normfs_wal_page_wf(&ring->pages[index]); */
	/*@ assert pages_wf_holds: normfs_wal_ring_pages_wf(ring); */

	/* Only scalars from here, so everything established above about the
	 * pages carries to the postconditions by frame. */
	ring->next_page_id = ring->next_page_id + 1u;
	ring->active = index;

	/*@ assert buffers_unmoved:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        ring->pages[k].buf == \at(ring->pages[k].buf, Pre) &&
	        ring->pages[k].cap == \at(ring->pages[k].cap, Pre); */

	/* Separation needs no hint of its own now. It is three statements about
	 * base pointers -- the ring, the descriptor array and the arena -- and
	 * rotation assigns none of them, so it survives by frame. This is what
	 * the arena bought: before it, sep quantified over every pair of page
	 * buffers, and transporting that across the call was the one goal that
	 * would not discharge. */
}

struct normfs_wal_ring_seek_result
normfs_wal_ring_seek(struct normfs_wal_ring *ring, uint64_t entry_id)
{
	struct normfs_wal_ring_seek_result r;
	size_t k;

	r.page_index = 0u;
	r.index = 0u;
	r.found = 0;

	/*@ loop invariant 0 <= k <= ring->page_count;
	    loop invariant r.found == 0;
	    loop assigns k, r;
	    loop variant ring->page_count - k;
	*/
	for (k = 0u; k < ring->page_count; k++) {
		struct normfs_wal_page_find_result f =
		    normfs_wal_page_find(&ring->pages[k], entry_id);
		if (f.found != 0) {
			r.page_index = k;
			r.index = f.index;
			r.found = 1;
			return r;
		}
	}

	return r;
}
