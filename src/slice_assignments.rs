use crate::health_check::HealthStatus;
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
        let mut servers = servers;
        servers.sort();
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SliceAssignmentsNext {
    pub servers: Vec<SocketAddr>,
    pub assignments: Vec<usize>,
}

impl SliceAssignmentsNext {
    pub fn new(servers: Vec<SocketAddr>) -> Self {
        let mut servers = servers;
        servers.sort();
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
        let mut servers = servers;
        servers.sort();

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

    fn find_best_moves(backends: &BTreeSet<Backend>) -> Vec<Move> {
        let mut moves = Vec::new();
        let mut servers: HashMap<SocketAddr, i32> = HashMap::new();
        let mut slice_loads: HashMap<u16, i32> = HashMap::new();
        let mut server_slices: HashMap<SocketAddr, HashMap<u16, i32>> = HashMap::new();

        // Collect current state
        for backend in backends {
            let addr = backend.addr.to_socket_addrs().unwrap().next().unwrap();
            let status = backend.ext.get::<HealthStatus>().unwrap();
            if let Some(usage) = status.inner.read().unwrap().usage.clone() {
                servers.insert(addr, usage.slices.values().map(|v| v.load).sum());
                server_slices.insert(addr, HashMap::new());
                for (slice_id, load) in usage.slices {
                    slice_loads.insert(slice_id as u16, load.load);
                    server_slices
                        .get_mut(&addr)
                        .unwrap()
                        .insert(slice_id as u16, load.load);
                }
            }
        }

        // Calculate average load
        let total_load: i32 = servers.values().sum();
        let avg_load = total_load as f32 / servers.len() as f32;
        let threshold = avg_load * Self::OVERLOAD_THRESHOLD;

        let s = servers.clone();
        // Find overloaded servers
        let mut overloaded: Vec<_> = s
            .iter()
            .filter(|(_, &load)| load as f32 > threshold)
            .collect();

        // Sort by load (most loaded first)
        overloaded.sort_by(|a, b| b.1.cmp(a.1));

        for (&hot_server, _) in overloaded {
            if moves.len() >= Self::MAX_MOVES_PER_CYCLE {
                break;
            }

            // Find the server's largest slice
            if let Some(server_slice_loads) = server_slices.get(&hot_server) {
                if let Some((&largest_slice, &slice_load)) =
                    server_slice_loads.iter().max_by_key(|(_, &load)| load)
                {
                    // Find least loaded server
                    if let Some((&target_server, _)) = servers
                        .iter()
                        .filter(|(&addr, _)| addr != hot_server)
                        .min_by_key(|(_, &load)| load)
                    {
                        // Calculate benefit
                        let old_imbalance = Self::calculate_imbalance(&servers);
                        let mut new_servers = servers.clone();

                        *new_servers.get_mut(&hot_server).unwrap() -= slice_load;
                        *new_servers.get_mut(&target_server).unwrap() += slice_load;

                        let new_imbalance = Self::calculate_imbalance(&new_servers);
                        let benefit = old_imbalance - new_imbalance;

                        moves.push(Move {
                            slice_id: largest_slice,
                            from_server: hot_server,
                            to_server: target_server,
                            benefit,
                        });

                        // Apply changes to servers map
                        let mut servers_clone = servers.clone();
                        *servers_clone.get_mut(&hot_server).unwrap() -= slice_load;
                        *servers_clone.get_mut(&target_server).unwrap() += slice_load;
                        servers = servers_clone;
                    }
                }
            }
        }

        moves
    }

    pub fn balance(backends: &BTreeSet<Backend>) {
        let best_moves = Self::find_best_moves(backends);
        println!("Top 5 most beneficial moves:");
        for mov in best_moves {
            println!(
                "Move slice {} from {} to {} (benefit: {:.3})",
                mov.slice_id, mov.from_server, mov.to_server, mov.benefit
            );
        }
    }

    fn calculate_imbalance(slices: &HashMap<SocketAddr, i32>) -> f32 {
        let total_load = slices.values().sum::<i32>();
        let max_load = *slices.values().max().unwrap();
        let mean_load = total_load / slices.len() as i32;
        max_load as f32 / mean_load as f32
    }
}
