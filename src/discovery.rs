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

use crate::health_check;

pub struct Discovery {
    port: u16,

    backends: BTreeSet<Backend>,
}

impl Discovery {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            backends: BTreeSet::new(),
        }
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
        let mut backends = BTreeSet::new();
        let response = resolver.txt_lookup("sliced.local.").await.unwrap();
        for answer in response.iter() {
            let mut backend = Backend::new(answer.to_string().as_str())?;
            backend.ext.insert(health_check::HealthStatus::new());
            backends.insert(backend);
        }

        return Ok((backends, HashMap::new()));

        // for ip in response.iter() {
        //     backends.insert(Backend::new(ip.to_string(), 80));
        // }

        // Ok((backends, health))
    }
}
