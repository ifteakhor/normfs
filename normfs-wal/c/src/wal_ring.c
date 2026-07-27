#include "normfs/wal_ring.h"

void
normfs_wal_ring_init(struct normfs_wal_ring *ring, struct normfs_wal_page *pages,
    size_t page_count, size_t page_size, uint64_t first_entry_id)
{
	ring->pages = pages;
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
	ring->next_page_id = ring->next_page_id + 1u;
	ring->active = index;
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
