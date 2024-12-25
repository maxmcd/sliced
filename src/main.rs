#![deny(clippy::all)]

mod discovery;
mod health_check;
mod selection;

use async_trait::async_trait;
use discovery::Discovery;
use health_check::WorkerHealthCheck;
use log::info;
use pingora::prelude::Opt;
use pingora::server::configuration::ServerConf;
use pingora::OkOrErr;
use pingora_core::server::Server;
use pingora_core::services::background::background_service;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_core::Result;
use pingora_load_balancing::selection::consistent::KetamaHashing;
use pingora_load_balancing::Backend;
use pingora_load_balancing::Backends;
use pingora_load_balancing::LoadBalancer;
use pingora_proxy::ProxyHttp;
use pingora_proxy::Session;
use selection::MemoryAddressSelection;
use std::collections::BTreeSet;
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;

pub fn main() {
    start_server();
}

fn start_server() {
    let mut server = Server::new(Some(Opt {
        upgrade: false,
        daemon: false,
        nocapture: false,
        test: false,
        conf: None,
    }))
    .unwrap();
    server.configuration = Arc::new(ServerConf {
        // What should this be? Maybe number of processes? But maybe we want to
        // limit how many cores this LB can occupy and leave the rest for Deno
        // workers.
        threads: 8,

        pid_file: String::from("/tmp/pingora.pid"),
        upgrade_sock: String::from("/tmp/pingora_upgrade.sock"),

        // These are all default values.
        version: 1,
        error_log: None,
        daemon: false,
        user: None,
        group: None,
        work_stealing: true,
        ca_file: None,
        grace_period_seconds: Some(10),
        graceful_shutdown_timeout_seconds: Some(10),
        client_bind_to_ipv4: vec![],
        client_bind_to_ipv6: vec![],
        upstream_keepalive_pool_size: 128,
        upstream_connect_offload_threadpools: None,
        upstream_connect_offload_thread_per_pool: None,
        upstream_debug_ssl_keylog: false,
    });
    server.bootstrap();

    let discovery = Box::new(Discovery::new(
        std::env::args()
            .nth(2)
            .expect("DNS Port number required")
            .parse()
            .unwrap(),
    ));
    let mut upstreams = LoadBalancer::from_backends(Backends::new(discovery));

    // Configure HTTP health check
    let hc = WorkerHealthCheck::new("sliced.local", false);

    upstreams.set_health_check(Box::new(hc));
    upstreams.health_check_frequency = Some(Duration::from_secs(1));
    upstreams.update_frequency = Some(Duration::from_secs(1));

    let background = background_service("health check", upstreams);

    let upstreams = background.task();
    let mut lb = pingora_proxy::http_proxy_service(&server.configuration, LB { upstreams });
    lb.add_tcp(
        format!(
            "0.0.0.0:{}",
            std::env::args().nth(1).expect("Port number required")
        )
        .as_str(),
    );

    server.add_service(lb);
    server.add_service(background);
    println!("Server started");

    server.run_forever();
}

struct LB {
    upstreams: Arc<LoadBalancer<KetamaHashing>>,
}

impl LB {}

struct Ctx {}

#[async_trait]
impl ProxyHttp for LB {
    type CTX = Ctx;
    fn new_ctx(&self) -> Self::CTX {
        Ctx {}
    }

    /// Handle the incoming request.
    ///
    /// In this phase, users can parse, validate, rate limit, perform access control and/or
    /// return a response for this request.
    ///
    /// If the user already sent a response to this request, an `Ok(true)` should be returned so that
    /// the proxy would exit. The proxy continues to the next phases when `Ok(false)` is returned.
    ///
    /// By default this filter does nothing and returns `Ok(false)`.
    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        Ok(false)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut pingora_http::RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        Ok(())
    }

    /// Define where the proxy should send the request to.
    ///
    /// The returned [HttpPeer] contains the information regarding where and how this request should
    /// be forwarded to.
    async fn upstream_peer(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let upstream = self
            .upstreams
            .select(session.get_header_bytes("X-User"), 256)
            .or_err(pingora::HTTPStatus(502), "No upstreams available")?;

        info!("upstream peer is: {:?}", upstream);

        let peer = Box::new(HttpPeer::new(upstream, false, "".to_string()));
        Ok(peer)
    }
}
