use async_trait::async_trait;
use pingora_core::connectors::http::Connector as HttpConnector;
use pingora_core::upstreams::peer::{HttpPeer, Peer};
use pingora_core::Error;
use pingora_core::Result;
use pingora_error::ErrorType::CustomCode;
use pingora_http::RequestHeader;
use pingora_http::ResponseHeader;
use pingora_load_balancing::health_check::HealthCheck;
use pingora_load_balancing::health_check::HealthObserveCallback;
use pingora_load_balancing::Backend;
use std::sync::{Arc, RwLock};
use std::time::Duration;

type Validator = Box<dyn Fn(&ResponseHeader) -> Result<()> + Send + Sync>;

pub struct WorkerHealthCheck {
    // Health check configuration
    consecutive_success: usize,
    consecutive_failure: usize,

    // HTTP specific fields
    peer_template: HttpPeer,
    reuse_connection: bool,
    req: RequestHeader,
    connector: HttpConnector,
    pub validator: Option<Validator>,
    port_override: Option<u16>,
    pub health_changed_callback: Option<HealthObserveCallback>,
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
            validator: None,
            port_override: None,
            health_changed_callback: None,
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
            validator: None,
            port_override: None,
            health_changed_callback: None,
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
}

impl HealthStatus {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HealthStatusInner {
                is_healthy: true, // default to healthy
                last_check: std::time::Instant::now(),
            })),
        }
    }
}

fn set_health(target: &Backend, is_healthy: bool) {
    let health = target
        .ext
        .get::<HealthStatus>()
        .expect("health status not found");
    let mut state = health.inner.write().unwrap();
    state.is_healthy = is_healthy;
    state.last_check = std::time::Instant::now();
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
        if let Some(validator) = self.validator.as_ref() {
            validator(resp)?;
        } else if resp.status != 200 {
            set_health(target, false);
            return Error::e_explain(
                CustomCode("non 200 code", resp.status.as_u16()),
                "during http healthcheck",
            );
        }

        set_health(target, true);

        // Drain response body
        while session.read_response_body().await?.is_some() {}

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
