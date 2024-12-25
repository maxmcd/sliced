use pingora_load_balancing::selection::BackendIter;
use pingora_load_balancing::selection::BackendSelection;
use pingora_load_balancing::Backend;
use std::collections::BTreeSet;
use std::sync::Arc;

pub struct MemoryAddressSelection {
    backends: Box<[Backend]>,
}

impl BackendSelection for MemoryAddressSelection {
    type Iter = MemoryAddressIterator;

    fn build(backends: &BTreeSet<Backend>) -> Self {
        MemoryAddressSelection {
            backends: Vec::from_iter(backends.iter().cloned()).into_boxed_slice(),
        }
    }

    fn iter(self: &Arc<Self>, _key: &[u8]) -> Self::Iter {
        MemoryAddressIterator {
            current: 0,
            selection: self.clone(),
        }
    }
}
pub struct MemoryAddressIterator {
    current: usize,
    selection: Arc<MemoryAddressSelection>,
}

impl BackendIter for MemoryAddressIterator {
    fn next(&mut self) -> Option<&Backend> {
        if self.current >= self.selection.backends.len() {
            return None;
        }

        let backend = &self.selection.backends[self.current];
        self.current += 1;
        self.selection.backends.iter().for_each(|backend| {
            println!(
                "backend: {:?}",
                backend.ext.get::<crate::health_check::HealthStatus>()
            );
        });
        Some(backend)
    }
}
