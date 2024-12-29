use pingora_load_balancing::selection::BackendIter;
use pingora_load_balancing::selection::BackendSelection;
use pingora_load_balancing::Backend;
use std::collections::BTreeSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

pub struct SliceSelection {
    backends: Box<[Backend]>,
}
impl BackendSelection for SliceSelection {
    type Iter = SliceBackendIterator;
    fn build(backends: &BTreeSet<Backend>) -> Self {
        SliceSelection {
            backends: Vec::from_iter(backends.iter().cloned()).into_boxed_slice(),
        }
    }
    fn iter(self: &Arc<Self>, key: &[u8]) -> Self::Iter {
        let mut state = DefaultHasher::new();
        key.hash(&mut state);
        let slice: u16 = (state.finish() % 1000) as u16;
        for backend in self.backends.iter() {
            let slices = backend.ext.get::<BTreeSet<u16>>().unwrap();
            if slices.contains(&slice) {
                return SliceBackendIterator {
                    backend: backend.clone(),
                };
            }
        }
        panic!("No backend found for slice: {}", slice);
    }
}
pub struct SliceBackendIterator {
    backend: Backend,
}
impl BackendIter for SliceBackendIterator {
    fn next(&mut self) -> Option<&Backend> {
        Some(&self.backend)
    }
}
