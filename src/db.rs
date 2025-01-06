use crate::slice_assignments::SliceAssignments;
use libsql::Builder;
use std::collections::BTreeSet;

const ASSIGNMENTS_TABLE: &str = "
CREATE TABLE IF NOT EXISTS assignments (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    timestamp INTEGER NOT NULL,
    data TEXT NOT NULL
)";

pub struct DB {
    pub conn: libsql::Connection,
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

    pub async fn update_servers(
        &self,
        servers: BTreeSet<String>,
    ) -> Result<SliceAssignments, libsql::Error> {
        let (mut assignments, timestamp) = self.get_assignments().await?;
        let servers: Vec<_> = servers.into_iter().map(|s| s.parse().unwrap()).collect();
        if assignments.servers.is_empty() {
            assignments = SliceAssignments::new(servers);
        } else {
            assignments.update(servers);
        }
        let resp = self.write_assignments(&assignments, timestamp).await?;
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

    async fn get_assignments(&self) -> Result<(SliceAssignments, i64), libsql::Error> {
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
        let assignments = SliceAssignments::new(servers);
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
