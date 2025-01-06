use crate::db::DB;
use async_trait::async_trait;
use hickory_resolver::config::NameServerConfigGroup;
use hickory_resolver::config::ResolverConfig;
use hickory_resolver::config::ResolverOpts;
use hickory_resolver::AsyncResolver;
use pingora_error::Result;
use pingora_load_balancing::discovery::ServiceDiscovery;
use pingora_load_balancing::Backend;
use std::collections::BTreeSet;
use std::collections::HashMap;

pub struct Discovery {
    port: u16,
    db: DB,
}

impl Discovery {
    pub fn new(port: u16, db: DB) -> Self {
        Self { port, db }
    }
}

#[async_trait]
impl ServiceDiscovery for Discovery {
    async fn discover(&self) -> Result<(BTreeSet<Backend>, HashMap<u64, bool>)> {
        let resolver = AsyncResolver::tokio(
            ResolverConfig::from_parts(
                None,
                vec![],
                NameServerConfigGroup::from_ips_clear(
                    &["127.0.0.1".parse().unwrap()],
                    self.port,
                    true,
                ),
            ),
            ResolverOpts::default(),
        );
        let response = resolver.txt_lookup("sliced.local.").await.unwrap();
        let backends_set: BTreeSet<_> = response.iter().map(|b| b.to_string()).collect();
        let assignments = self.db.update_servers(backends_set).await.unwrap();
        let backends = assignments.to_backends();

        println!("backends: {:?}", backends);

        return Ok((backends, HashMap::new()));
    }
}
