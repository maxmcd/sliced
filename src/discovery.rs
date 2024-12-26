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
}

impl Discovery {
    pub fn new(port: u16) -> Self {
        Self { port }
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
            backends.insert(Backend::new(answer.to_string().as_str())?);
        }

        return Ok((backends, HashMap::new()));
    }
}
