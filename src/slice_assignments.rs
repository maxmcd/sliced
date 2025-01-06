use crate::health_check::HealthStatus;
use log::info;
use pingora_ketama::Bucket;
use pingora_ketama::Continuum;
use pingora_load_balancing::Backend;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;

pub const NUM_SLICES: u16 = 100;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SliceAssignments {
    pub servers: Vec<SocketAddr>,
    pub assignments: Vec<usize>,
}

impl SliceAssignments {
    pub fn new(servers: Vec<SocketAddr>) -> Self {
        // Use consistent hashing for initial slice assignment. This is mostly
        // to ensure determinism in testing and could easily be replaced with
        // another method.
        let buckets: Vec<_> = servers
            .iter()
            .filter_map(|s| s.ip().is_ipv4().then_some(Bucket::new(*s, 1)))
            .collect();
        let ring = Continuum::new(&buckets);
        let assignments = (0..NUM_SLICES)
            .map(|i| {
                let addr = ring.get_addr(&mut ring.node_idx(&[i as u8])).unwrap();
                servers.iter().position(|s| s == addr).unwrap()
            })
            .collect();
        Self {
            servers,
            assignments,
        }
    }
    pub fn update(&mut self, servers: Vec<SocketAddr>) {
        // If servers list is identical, no changes needed
        if servers == self.servers {
            return;
        }

        // Create buckets for consistent hashing
        let buckets: Vec<_> = servers
            .iter()
            .filter_map(|s| s.ip().is_ipv4().then_some(Bucket::new(*s, 1)))
            .collect();
        let ring = Continuum::new(&buckets);

        // Find servers that were removed
        let removed_servers: Vec<_> = self
            .servers
            .iter()
            .enumerate()
            .filter(|(_, s)| !servers.contains(s))
            .map(|(i, _)| i)
            .collect();

        // Re-assign only slices that were assigned to removed servers
        let mut assignments = self.assignments.clone();
        for (i, assignment) in assignments.iter_mut().enumerate() {
            if removed_servers.contains(assignment) {
                let addr = ring.get_addr(&mut ring.node_idx(&[i as u8])).unwrap();
                *assignment = servers.iter().position(|s| s == addr).unwrap();
            }
        }

        self.servers = servers;
        self.assignments = assignments;
    }

    pub fn move_load(&mut self) {
        let moves = Balance::find_best_moves(&self.to_backends());
        if !moves.is_empty() {
            info!("Top most beneficial moves:");
            for mov in moves {
                info!(
                    "Move slice {} from {} to {} (benefit: {:.3})",
                    mov.slice_id, mov.from_server, mov.to_server, mov.benefit
                );
                self.assignments[mov.slice_id as usize] = self
                    .servers
                    .iter()
                    .position(|s| s == &mov.to_server)
                    .unwrap();
            }
        } else {
            info!("No moves found");
        }
    }

    pub fn to_backends(&self) -> BTreeSet<Backend> {
        let mut backends = BTreeSet::new();
        for (i, server) in self.servers.iter().enumerate() {
            let mut backend = Backend::new(&server.to_string()).unwrap();
            let mut slices = BTreeSet::new();
            for (slice_idx, &server_idx) in self.assignments.iter().enumerate() {
                if server_idx == i {
                    slices.insert(slice_idx as u16);
                }
            }
            backend.ext.insert(slices);
            backend.ext.insert(HealthStatus::new());
            backends.insert(backend);
        }
        backends
    }
}

#[derive(Debug, Clone)]
struct Move {
    slice_id: u16,
    from_server: SocketAddr,
    to_server: SocketAddr,
    benefit: f32,
}

impl PartialOrd for Move {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.benefit.partial_cmp(&other.benefit)
    }
}

impl PartialEq for Move {
    fn eq(&self, other: &Self) -> bool {
        self.benefit == other.benefit
    }
}

struct Balance {}

impl Balance {
    // Threshold above average load that triggers rebalancing (20%)
    const OVERLOAD_THRESHOLD: f32 = 1.2;
    // Maximum number of moves per rebalancing cycle
    const MAX_MOVES_PER_CYCLE: usize = 3;

    fn collect_server_stats(
        backends: &BTreeSet<Backend>,
    ) -> (
        HashMap<SocketAddr, u32>,
        HashMap<SocketAddr, HashMap<u16, u32>>,
    ) {
        let mut servers = HashMap::new();
        let mut server_slices = HashMap::new();

        for backend in backends {
            let addr = backend.addr.to_socket_addrs().unwrap().next().unwrap();
            let status = backend.ext.get::<HealthStatus>().unwrap();

            if let Some(usage) = status.inner.read().unwrap().usage.clone() {
                servers.insert(addr, usage.slices.values().map(|v| v.load).sum());
                let mut slices = HashMap::new();
                for (slice_id, load) in usage.slices {
                    slices.insert(slice_id, load.load);
                }
                server_slices.insert(addr, slices);
            }
        }

        (servers, server_slices)
    }

    pub fn find_best_moves(backends: &BTreeSet<Backend>) -> Vec<Move> {
        let mut moves = Vec::new();
        let (mut servers, server_slices) = Self::collect_server_stats(backends);

        // Calculate threshold for overloaded servers
        let avg_load = servers.values().sum::<u32>() as f32 / servers.len() as f32;
        let threshold = avg_load * Self::OVERLOAD_THRESHOLD;

        let o = servers.clone();
        // Find and sort overloaded servers
        let mut overloaded: Vec<_> = o
            .iter()
            .filter(|(_, &load)| load as f32 > threshold)
            .collect();
        overloaded.sort_by(|a, b| b.1.cmp(a.1));

        for (&hot_server, _) in overloaded {
            if moves.len() >= Self::MAX_MOVES_PER_CYCLE {
                break;
            }

            // Find largest slice from hot server
            if let Some((&largest_slice, &slice_load)) = server_slices
                .get(&hot_server)
                .and_then(|slices| slices.iter().max_by_key(|(_, &load)| load))
            {
                // Find least loaded target server
                if let Some((&target_server, _)) = servers
                    .iter()
                    .filter(|(&addr, _)| addr != hot_server)
                    .min_by_key(|(_, &load)| load)
                {
                    let old_imbalance = Self::calculate_imbalance(&servers);

                    // Update server loads
                    *servers.get_mut(&hot_server).unwrap() -= slice_load;
                    *servers.get_mut(&target_server).unwrap() += slice_load;

                    moves.push(Move {
                        slice_id: largest_slice,
                        from_server: hot_server,
                        to_server: target_server,
                        benefit: old_imbalance - Self::calculate_imbalance(&servers),
                    });
                }
            }
        }

        moves
    }

    fn calculate_imbalance(slices: &HashMap<SocketAddr, u32>) -> f32 {
        let total_load = slices.values().sum::<u32>();
        let max_load = *slices.values().max().unwrap();
        let mean_load = total_load / slices.len() as u32;
        max_load as f32 / mean_load as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, RwLock};

    fn create_test_backend(addr: &str, slice_loads: Vec<(u16, u32)>) -> Backend {
        let mut backend = Backend::new(addr).unwrap();
        let mut status = HealthStatus::new();

        // Create usage data
        let mut usage = crate::health_check::Usage {
            slices: HashMap::new(),
        };
        for (slice_id, load) in slice_loads {
            usage
                .slices
                .insert(slice_id, crate::health_check::SliceUsage { load });
        }

        status.inner = Arc::new(RwLock::new(crate::health_check::HealthStatusInner {
            usage: Some(usage),
            is_healthy: true,
            last_check: std::time::Instant::now(),
        }));

        backend.ext.insert(status);
        backend
    }

    #[test]
    fn test_collect_server_stats() {
        let mut backends = BTreeSet::new();

        // Add two test backends with different loads
        backends.insert(create_test_backend(
            "127.0.0.1:8001",
            vec![(0, 100), (1, 200)],
        ));
        backends.insert(create_test_backend(
            "127.0.0.1:8002",
            vec![(2, 50), (3, 150)],
        ));

        let (servers, server_slices) = Balance::collect_server_stats(&backends);

        // Check server total loads
        assert_eq!(servers.len(), 2);
        assert_eq!(servers[&"127.0.0.1:8001".parse().unwrap()], 300); // 100 + 200
        assert_eq!(servers[&"127.0.0.1:8002".parse().unwrap()], 200); // 50 + 150

        // Check individual slice loads
        let server1_slices = &server_slices[&"127.0.0.1:8001".parse().unwrap()];
        assert_eq!(server1_slices[&0], 100);
        assert_eq!(server1_slices[&1], 200);

        let server2_slices = &server_slices[&"127.0.0.1:8002".parse().unwrap()];
        assert_eq!(server2_slices[&2], 50);
        assert_eq!(server2_slices[&3], 150);
    }

    #[test]
    fn test_find_best_moves() {
        let mut backends = BTreeSet::new();

        // Create an overloaded server (total load 900)
        backends.insert(create_test_backend(
            "127.0.0.1:8001",
            vec![(0, 400), (1, 500)],
        ));
        // Create an underloaded server (total load 300)
        backends.insert(create_test_backend(
            "127.0.0.1:8002",
            vec![(2, 150), (3, 150)],
        ));

        let moves = Balance::find_best_moves(&backends);

        assert!(!moves.is_empty());
        let first_move = &moves[0];

        // The largest slice (500) should be moved from the overloaded to underloaded server
        assert_eq!(first_move.slice_id, 1);
        assert_eq!(first_move.from_server, "127.0.0.1:8001".parse().unwrap());
        assert_eq!(first_move.to_server, "127.0.0.1:8002".parse().unwrap());
        assert!(first_move.benefit > 0.0);
    }

    #[test]
    fn test_calculate_imbalance() {
        let mut servers = HashMap::new();
        servers.insert("127.0.0.1:8001".parse().unwrap(), 1000);
        servers.insert("127.0.0.1:8002".parse().unwrap(), 500);
        servers.insert("127.0.0.1:8003".parse().unwrap(), 500);

        let imbalance = Balance::calculate_imbalance(&servers);

        // Max load is 1000, mean load is 666.67
        // Expected imbalance is approximately 1.5
        assert!((imbalance - 1.5).abs() < 0.1);
    }

    #[test]
    fn test_no_moves_when_balanced() {
        let mut backends = BTreeSet::new();

        // Create three similarly loaded servers
        backends.insert(create_test_backend(
            "127.0.0.1:8001",
            vec![(0, 100), (1, 100)],
        ));
        backends.insert(create_test_backend(
            "127.0.0.1:8002",
            vec![(2, 100), (3, 100)],
        ));
        backends.insert(create_test_backend(
            "127.0.0.1:8003",
            vec![(4, 100), (5, 100)],
        ));

        let moves = Balance::find_best_moves(&backends);
        println!("moves: {:?}", moves);
        assert!(moves.is_empty());
    }
}
