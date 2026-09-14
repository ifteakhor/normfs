use super::BoundedMap;

#[test]
fn the_oldest_entry_goes_when_the_cap_is_reached() {
    let mut map = BoundedMap::new(2);
    map.insert("a", 1);
    map.insert("b", 2);
    map.insert("c", 3);
    assert_eq!(map.len(), 2);
    assert_eq!(map.get(&"a"), None);
    assert_eq!(map.get(&"b"), Some(&2));
    assert_eq!(map.get(&"c"), Some(&3));
}

#[test]
fn reinserting_a_key_does_not_count_twice() {
    let mut map = BoundedMap::new(2);
    map.insert("a", 1);
    map.insert("a", 2);
    map.insert("b", 3);
    assert_eq!(map.len(), 2);
    assert_eq!(map.get(&"a"), Some(&2));
}

#[test]
fn a_removed_key_does_not_evict_a_live_one_later() {
    let mut map = BoundedMap::new(2);
    map.insert("a", 1);
    map.remove(&"a");
    map.insert("b", 2);
    map.insert("c", 3);
    assert_eq!(map.get(&"b"), Some(&2));
    assert_eq!(map.get(&"c"), Some(&3));
}
