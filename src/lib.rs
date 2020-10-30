use std::borrow::Borrow;
use std::hash::{BuildHasher, Hash, Hasher};
use std::mem;
use std::sync::{Arc, RwLock};
use std::vec::Vec;

pub struct Carta<K, V, B>
    where B: BuildHasher,
{
    hash_builder: B,
    buckets: Vec<RwLock<Vec<(K, RwLock<Arc<V>>)>>>,
}

impl<K, V, B> Carta<K, V, B>
    where B: BuildHasher,
          K: Hash + Eq,
{
    /// Initializes an empty concurrent hash map.
    pub fn new_with_hash_builder(hash_builder: B) -> Self {
        // Initialize an empty vec to store the hash buckets, each of which
        // will store key-value pairs that map to that bucket.
        let buckets = (0..2048 * 16).map(|_| RwLock::new(Vec::new())).collect();
        Self { hash_builder, buckets }
    }

    /// Inserts a key-value pair into the map.
    ///
    /// If the key was not already present in the map, `None` is returned.
    /// If the key was already present in the map, the value is updated and
    /// the previous value is returned.
    pub fn insert(&self, key: K, value: V) -> Option<Arc<V>> {
        let index = self.get_index(&key);
        let mut bucket = self.buckets[index].write().unwrap();
        for (k, v) in bucket.iter_mut() {
            if *k != key { continue; }
            let mut v = v.write().unwrap();
            return Some(mem::replace(&mut *v, Arc::new(value)));
        }
        bucket.push((key, RwLock::new(Arc::new(value))));
        None
    }

    /// Returns a reference to the value corresponding to the key.
    pub fn get<Q>(&self, key: &Q) -> Option<Arc<V>>
        where K: Borrow<Q>,
              Q: Hash + PartialEq,
    {
        let index = self.get_index(key);
        let bucket = self.buckets[index].read().unwrap();
        for (k, ref v) in bucket.iter() {
            if k.borrow() == key { return Some(v.read().unwrap().clone()) }
        }
        None
    }

    pub fn remove<Q>(&self, key: &Q) -> Option<Arc<V>>
        where K: Borrow<Q>,
              Q: Hash + PartialEq,
    {
        let index = self.get_index(key);
        let mut bucket = self.buckets[index].write().unwrap();
        if let Some(position) = bucket.iter().position(|(k, _)| (*k).borrow() == key) {
            return Some(bucket.remove(position).1.into_inner().unwrap())
        }
        None
    }

    // TODO: make this take &Q
    pub fn update(&self, key: K, f: impl Fn(&mut Arc<V>)) -> Option<Arc<V>> {
        let index = self.get_index(&key);
        let mut bucket = self.buckets[index].write().unwrap();
        for (k, v) in bucket.iter_mut() {
            if *k != key { continue; }
            let mut v = v.write().unwrap();
            f(&mut *v);
            return Some(v.clone());
        }
        None
    }

    fn get_index<Q>(&self, key: &Q) -> usize
        where K: Borrow<Q>,
              Q: Hash + PartialEq,
    {
        let hash = {
            // Build the hasher since everytime we need to start a fresh hash
            // value we need a hasher with a clear internal state.
            let mut hasher = self.hash_builder.build_hasher();
            key.hash(&mut hasher);
            hasher.finish()
        };
        (hash % self.buckets.len() as u64) as usize
    }
}
