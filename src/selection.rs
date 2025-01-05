use pingora_load_balancing::selection::BackendIter;
use pingora_load_balancing::selection::BackendSelection;
use pingora_load_balancing::Backend;
use std::collections::BTreeSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use crate::slice_assignments::NUM_SLICES;

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
        let slice: u16 = (state.finish() % NUM_SLICES as u64) as u16;
        if self.backends.is_empty() {
            return SliceBackendIterator { backend: None };
        }
        for backend in self.backends.iter() {
            let slices = backend.ext.get::<BTreeSet<u16>>().unwrap();
            if slices.contains(&slice) {
                return SliceBackendIterator {
                    backend: Some(backend.clone()),
                };
            }
        }
        panic!("No backend found for slice: {}", slice);
    }
}
pub struct SliceBackendIterator {
    backend: Option<Backend>,
}
impl BackendIter for SliceBackendIterator {
    fn next(&mut self) -> Option<&Backend> {
        self.backend.as_ref()
    }
}
