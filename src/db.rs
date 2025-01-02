use pingora_ketama::{Bucket, Continuum};
use pingora_load_balancing::Backend;
use std::{collections::BTreeSet, net::SocketAddr};

use libsql::{de, Builder};

use crate::health_check::HealthStatus;
#[derive(Debug, serde::Deserialize)]
pub struct SliceServer {
    pub id: i64,
    pub server_id: String,
}

pub const NUM_SLICES: u16 = 100;

const ASSIGNMENTS_TABLE: &str = "
CREATE TABLE IF NOT EXISTS assignments (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    timestamp INTEGER NOT NULL,
    data TEXT NOT NULL
)";

pub struct DB {
    pub conn: libsql::Connection,
}

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

fn new_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

impl DB {
    pub async fn new(memory: bool) -> Result<Self, libsql::Error> {
        let db = Builder::new_local(if memory { ":memory:" } else { "server.sqlite" })
            .build()
            .await?;
        let conn = db.connect()?;
        conn.execute(ASSIGNMENTS_TABLE, ()).await?;

        conn.execute(
            "INSERT OR IGNORE INTO assignments (id, timestamp, data) VALUES (1, 0, '{\"servers\":[], \"assignments\":[], \"timestamp\":0}')",
            ()
        ).await?;

        Ok(Self { conn })
    }

    pub async fn new_servers(
        &self,
        servers: BTreeSet<String>,
        version_timestamp: i64,
    ) -> Result<SliceAssignments, libsql::Error> {
        let mut assignments =
            SliceAssignments::new(servers.into_iter().map(|s| s.parse().unwrap()).collect());

        let resp = self
            .write_assignments(&assignments, version_timestamp)
            .await?;

        if !resp.0 {
            // Another server handled the migration, fetch the new assignments.
            assignments = self.get_assignments().await?.0;
        }
        Ok(assignments)
    }

    async fn write_assignments(
        &self,
        assignments: &SliceAssignments,
        timestamp: i64,
    ) -> Result<(bool, i64), libsql::Error> {
        let json = serde_json::to_string(assignments)
            .map_err(|e| libsql::Error::ConnectionFailed(e.to_string()))?;
        let new_timestamp = new_timestamp();

        let rows_affected = self
            .conn
            .execute(
                "UPDATE assignments SET timestamp = ?, data = ? WHERE id = 1 AND timestamp = ?",
                (new_timestamp, json, timestamp),
            )
            .await?;

        Ok((rows_affected > 0, new_timestamp))
    }

    pub async fn get_assignments(&self) -> Result<(SliceAssignments, i64), libsql::Error> {
        let mut result = self
            .conn
            .query("SELECT timestamp, data FROM assignments WHERE id = 1", ())
            .await?;

        let row = result.next().await?.unwrap();

        let timestamp: i64 = row.get(0)?;
        let data: String = row.get(1)?;

        let assignments: SliceAssignments = serde_json::from_str(&data)
            .map_err(|e| libsql::Error::ConnectionFailed(e.to_string()))?;

        Ok((assignments, timestamp))
    }
}

#[derive(Debug)]
pub struct ProbeResponse {
    slice: u16,
    rif: u32,
    median_latency: u32,
    cpu: u32,
    memory: u32,
}

// Option 2: Keep fixed time window
const MAX_HISTORY_AGE_MS: u32 = 1000 * 60 * 10; // 10 minutes

#[derive(Debug, Copy, Clone)]
pub struct Slice {
    pub requests_in_flight: [u32; 50],
    pub latency: [u32; 50],
    pub cpu: [u32; 50],
    pub memory: [u32; 50],
}

#[derive(Debug)]
pub struct SliceLoad {
    slices: [Slice; NUM_SLICES as usize],
}

impl SliceLoad {
    pub fn new() -> Self {
        // Initialize array with None values
        Self {
            slices: [Slice {
                requests_in_flight: [0; 50],
                latency: [0; 50],
                cpu: [0; 50],
                memory: [0; 50],
            }; NUM_SLICES as usize],
        }
    }

    fn current_slot() -> usize {
        let current_time_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u32;

        let slot_duration = MAX_HISTORY_AGE_MS / 50;
        ((current_time_ms / slot_duration) % 50) as usize
    }

    pub fn update(&mut self, response: ProbeResponse) {
        self.slices[response.slice as usize].requests_in_flight[Self::current_slot()] =
            response.rif;
        self.slices[response.slice as usize].latency[Self::current_slot()] =
            response.median_latency;
        self.slices[response.slice as usize].cpu[Self::current_slot()] = response.cpu;
        self.slices[response.slice as usize].memory[Self::current_slot()] = response.memory;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_db() {
        let db = DB::new(true).await.expect("Failed to create DB");

        let read_assignments = db
            .get_assignments()
            .await
            .expect("Failed to get assignments");

        println!("{:?}", read_assignments);

        // Create test assignments
        let servers = vec![
            "127.0.0.1:8080".parse().unwrap(),
            "127.0.0.1:8081".parse().unwrap(),
            "127.0.0.1:8082".parse().unwrap(),
            "127.0.0.1:8083".parse().unwrap(),
        ];
        let mut assignments = SliceAssignments::new(servers);
        let timestamp = 0;
        // Write assignments
        let (write_success, new_timestamp) = db
            .write_assignments(&assignments, timestamp)
            .await
            .expect("Failed to write assignments");
        assert!(write_success, "Initial write should succeed");

        // Read assignments back
        let (read_assignments, timestamp) = db
            .get_assignments()
            .await
            .expect("Failed to get assignments");

        println!("{:?}", read_assignments.assignments);

        // Verify the data matches
        assert_eq!(read_assignments.servers, assignments.servers);
        assert_eq!(read_assignments.assignments, assignments.assignments);
        assert!(timestamp > 0, "Timestamp should be set");

        // Create test assignments
        let servers = vec![
            "127.0.0.1:8080".parse().unwrap(),
            "127.0.0.1:8081".parse().unwrap(),
            "127.0.0.1:8082".parse().unwrap(),
        ];
        let assignments = SliceAssignments::new(servers);

        let (write_success, _) = db
            .write_assignments(&assignments, new_timestamp)
            .await
            .expect("Failed to write assignments");
        assert!(write_success, "Initial write should succeed");

        // Read assignments back
        let (read_assignments, _) = db
            .get_assignments()
            .await
            .expect("Failed to get assignments");

        println!("{:?}", read_assignments.assignments);
    }
}
