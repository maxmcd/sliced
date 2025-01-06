use async_trait::async_trait;
use pingora_core::connectors::http::Connector as HttpConnector;
use pingora_core::upstreams::peer::{HttpPeer, Peer};
use pingora_core::Error;
use pingora_core::Result;
use pingora_error::ErrorType::CustomCode;
use pingora_http::RequestHeader;
use pingora_load_balancing::health_check::HealthCheck;
use pingora_load_balancing::Backend;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

pub struct WorkerHealthCheck {
    // Health check configuration
    consecutive_success: usize,
    consecutive_failure: usize,

    // HTTP specific fields
    peer_template: HttpPeer,
    reuse_connection: bool,
    req: RequestHeader,
    connector: HttpConnector,
    port_override: Option<u16>,
}

impl Default for WorkerHealthCheck {
    fn default() -> Self {
        // Create default HTTP request
        let mut req = RequestHeader::build("GET", b"/health", None).unwrap();
        req.append_header("Host", "localhost").unwrap();

        // Create peer template
        let mut peer_template = HttpPeer::new("0.0.0.0:1", false, String::new());
        peer_template.options.connection_timeout = Some(Duration::from_secs(1));
        peer_template.options.read_timeout = Some(Duration::from_secs(1));

        WorkerHealthCheck {
            consecutive_success: 1,
            consecutive_failure: 1,
            peer_template,
            connector: HttpConnector::new(None),
            reuse_connection: false,
            req,
            port_override: None,
        }
    }
}

impl WorkerHealthCheck {
    pub fn new(host: &str, tls: bool) -> Self {
        let mut req = RequestHeader::build("GET", b"/", None).unwrap();
        req.append_header("Host", host).unwrap();
        let sni = if tls { host.into() } else { String::new() };
        let mut peer_template = HttpPeer::new("0.0.0.0:1", tls, sni);
        peer_template.options.connection_timeout = Some(Duration::from_secs(1));
        peer_template.options.read_timeout = Some(Duration::from_secs(1));
        WorkerHealthCheck {
            consecutive_success: 1,
            consecutive_failure: 1,
            peer_template,
            connector: HttpConnector::new(None),
            reuse_connection: false,
            req,
            port_override: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HealthStatus {
    pub inner: Arc<RwLock<HealthStatusInner>>,
}

#[derive(Clone, Debug)]
pub struct HealthStatusInner {
    pub is_healthy: bool,
    pub last_check: std::time::Instant,
    pub usage: Option<Usage>,
}

impl HealthStatus {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HealthStatusInner {
                is_healthy: true, // default to healthy
                last_check: std::time::Instant::now(),
                usage: None,
            })),
        }
    }
}

fn set_health(target: &Backend, is_healthy: bool, usage: Option<Usage>) {
    let health = target
        .ext
        .get::<HealthStatus>()
        .expect("health status not found");
    let mut state = health.inner.write().unwrap();
    state.is_healthy = is_healthy;
    if usage.is_some() {
        // TODO: Might mean we're dealing with stale data
        state.usage = usage;
    }
    state.last_check = std::time::Instant::now();
}

/// Usage is a map of slice index to a "load" number that can be whatever you
/// want.
#[derive(Clone, Debug, serde::Deserialize)]
pub struct Usage {
    pub slices: HashMap<u16, SliceUsage>,
}
#[derive(Clone, Debug, serde::Deserialize)]
pub struct SliceUsage {
    pub load: u32,
}

#[async_trait]
impl HealthCheck for WorkerHealthCheck {
    fn health_threshold(&self, success: bool) -> usize {
        if success {
            self.consecutive_success
        } else {
            self.consecutive_failure
        }
    }

    async fn check(&self, target: &Backend) -> Result<()> {
        println!("checking health of {}", target.addr);
        // Clone peer template and set target address
        let mut peer = self.peer_template.clone();
        peer._address = target.addr.clone();
        if let Some(port) = self.port_override {
            peer._address.set_port(port);
        }

        // Establish HTTP session
        let session = self.connector.get_http_session(&peer).await?;
        let mut session = session.0;

        // Send request
        let req = Box::new(self.req.clone());
        session.write_request_header(req).await?;

        // Set read timeout if configured
        if let Some(read_timeout) = peer.options.read_timeout {
            session.set_read_timeout(read_timeout);
        }

        // Read response
        session.read_response_header().await?;
        let resp = session.response_header().expect("just read");

        // Validate response
        // if let Some(validator) = self.validator.as_ref() {
        //     validator(resp)?;
        // }
        let status = resp.status;

        let mut body: Vec<u8> = Vec::new();

        let mut usage: Option<Usage> = None;
        // Drain response body
        while let Some(bytes) = session.read_response_body().await? {
            // TODO: make sure this is the way to do this
            // TODO: bail when it's too big?
            body.append(&mut bytes.try_into_mut().unwrap().to_vec());
        }

        if let Ok(_usage) = serde_json::from_slice::<Usage>(&body[..]) {
            usage = Some(_usage)
        }

        if status != 200 {
            set_health(target, false, usage);
            return Error::e_explain(
                CustomCode("non 200 code", status.as_u16()),
                "during http healthcheck",
            );
        }
        set_health(target, true, usage);

        // Handle connection reuse
        if self.reuse_connection {
            let idle_timeout = peer.idle_timeout();
            self.connector
                .release_http_session(session, &peer, idle_timeout)
                .await;
        }

        Ok(())
    }
}
