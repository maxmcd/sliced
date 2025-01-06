#![deny(clippy::all)]

mod db;
mod discovery;
mod health_check;
mod selection;
mod slice_assignments;
use crate::db::DB;
use crate::discovery::Discovery;
use crate::health_check::WorkerHealthCheck;
use crate::selection::SliceSelection;
use async_trait::async_trait;
use log::info;
use pingora::prelude::Opt;
use pingora::server::configuration::ServerConf;
use pingora::OkOrErr;
use pingora_core::server::Server;
use pingora_core::services::background::background_service;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_core::Result;
use pingora_load_balancing::Backends;
use pingora_load_balancing::LoadBalancer;
use pingora_proxy::ProxyHttp;
use pingora_proxy::Session;
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

        pid_file: String::from(""),
        upgrade_sock: String::from(""),

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

    let db = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(DB::new(false))
        .unwrap();
    let dns_port = std::env::args()
        .nth(2)
        .expect("DNS Port number required")
        .parse()
        .unwrap();

    let discovery = Box::new(Discovery::new(dns_port, db));
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
    upstreams: Arc<LoadBalancer<SliceSelection>>,
}

impl LB {}

struct Ctx {}

#[async_trait]
impl ProxyHttp for LB {
    type CTX = Ctx;
    fn new_ctx(&self) -> Self::CTX {
        Ctx {}
    }

    /// Define where the proxy should send the request to.
    ///
    /// The returned [HttpPeer] contains the information regarding where and how this request should
    /// be forwarded to.
    async fn upstream_peer(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX,
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
