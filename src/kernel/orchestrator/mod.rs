use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::api::server::ApiServer;
use crate::auth::jwt::{JwtValidator, MIN_JWT_SECRET_BYTES};
use crate::bridge::{Bridge, BridgeHandle};
use crate::events::bus::{run_retry_worker, EventBus};
use crate::events::store::EventStore;
use crate::ipc::protocol::MessageRouter;
use crate::ipc::server::UdsServer;
use crate::plugins::loader::PluginLoader;
use crate::plugins::manager::PluginManager;
use crate::plugins::registry::PluginRegistry;
use crate::plugins::supervisor::PluginSupervisor;
use crate::utils::config::{resolve_device_id, Config, Role};

mod shutdown;

pub struct Kernel;

impl Kernel {
    pub async fn run_with_shutdown<F>(config: Config, shutdown: F) -> anyhow::Result<()>
    where
        F: Future<Output = ()>,
    {
        let registry = Arc::new(PluginRegistry::new());
        let event_bus = Arc::new(EventBus::new());
        Self::run_with_components(config, registry, event_bus, shutdown).await
    }

    pub async fn run_with_components<F>(
        config: Config,
        registry: Arc<PluginRegistry>,
        event_bus: Arc<EventBus>,
        shutdown: F,
    ) -> anyhow::Result<()>
    where
        F: Future<Output = ()>,
    {
        crate::metrics::init_metrics();

        let event_store = match EventStore::new(&config.data_dir) {
            Ok(s) => {
                let s = Arc::new(s);
                info!(path = %config.data_dir.display(), "EventStore opened");
                Some(s)
            }
            Err(e) => {
                tracing::warn!("EventStore unavailable — at-least-once delivery disabled: {e}");
                None
            }
        };

        // attach, don't replace: callers hold this Arc (tests publish through it)
        if let Some(store) = &event_store {
            event_bus.set_store(Arc::clone(store));
        }

        let (router_tx, router_rx) = mpsc::channel(config.router_channel_capacity);
        let ws_router_tx = router_tx.clone();
        let (_server_handle, disconnect_rx) = UdsServer::start(
            Path::new(&config.socket_path),
            router_tx,
            config.max_connections,
            config.fragment_timeout_secs,
            config.max_reassembly_streams,
        )
        .await?;
        let (ws_disconnect_tx, ws_disconnect_rx) = mpsc::channel::<u64>(64);
        info!("UDS server listening on {}", config.socket_path);

        if let Some(secret) = config.jwt_secret.as_deref() {
            if secret.len() < MIN_JWT_SECRET_BYTES {
                anyhow::bail!(
                    "jwt_secret is {} bytes, must be at least {MIN_JWT_SECRET_BYTES} bytes \
                     (HS256 secrets shorter than this are brute-forceable)",
                    secret.len()
                );
            }
        }
        let jwt_validator = config.jwt_secret.as_deref().map(|s| {
            info!("JWT auth enabled");
            Arc::new(JwtValidator::with_audience(
                s.as_bytes(),
                config.jwt_audience.clone(),
            ))
        });
        if jwt_validator.is_none() {
            if !config.allow_no_auth {
                anyhow::bail!(
                    "refusing to start without authentication: set `jwt_secret`, or set \
                     `allow_no_auth: true` in config to run without auth (insecure)"
                );
            }
            tracing::warn!(
                "JWT auth DISABLED (allow_no_auth) — any local process can register as any \
                 plugin; do not use in production"
            );
        }

        // D-07: TLS is on by default; resolve (or auto-generate) the cert pair
        // before any listener starts so a half-configured tls_* fails the boot
        // instead of silently downgrading to plaintext.
        let (tls_cert_path, tls_key_path) = crate::utils::tls::resolve_tls_paths(&config)?;

        // D-07: a host that actually serves remote devices has auth on — bind
        // beyond loopback only then. With allow_no_auth the API stays local
        // regardless of role (exposing an unauthenticated control plane would
        // be a footgun), overridable via explicit `bind:`.
        let bind_ip: std::net::IpAddr = if let Some(bind) = &config.bind {
            bind.parse()
                .map_err(|_| anyhow::anyhow!("invalid bind address '{bind}'"))?
        } else if config.role == Role::Host && config.jwt_secret.is_some() {
            std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
        } else {
            if config.role == Role::Host {
                warn!(
                    "role: host without jwt_secret — API stays bound to 127.0.0.1; \
                     set jwt_secret to expose the network path to remote devices"
                );
            }
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        };

        let kernel_start = std::time::Instant::now();
        let config_path = config.config_file.clone();
        // Frame-MAC key material: the same secret used for JWT, or None when
        // running without auth (then frames are CRC-only, unchanged).
        let mac_secret = config
            .jwt_secret
            .as_ref()
            .map(|s| Arc::new(s.as_bytes().to_vec()));
        // E-01: per-device credential store (encrypted at rest under a key
        // derived from jwt_secret). Always present when auth is on; the router
        // and the WS gateway only consult it for device-scoped connections.
        let device_store = config.jwt_secret.as_ref().map(|s| {
            Arc::new(crate::auth::device_store::DeviceStore::new(
                &config.data_dir,
                s,
            ))
        });
        // T-04: operator-declared permission allowlist per plugin id, used to
        // clamp JWT-claimed permissions at registration (see protocol.rs).
        let config_permissions = Arc::new(
            config
                .plugins
                .iter()
                .map(|d| (d.id.clone(), d.permissions.clone()))
                .collect::<std::collections::HashMap<_, _>>(),
        );
        // D-06: in client role, mirror the configured capabilities to the host
        // and hand the router the relay handle (frames whose target is not in
        // the local registry fall through to the host).
        let bridge_handle = if config.role == Role::Client {
            if let Some(bridge_cfg) = &config.bridge {
                let handle = BridgeHandle::new();
                let bridge = Bridge::new(
                    bridge_cfg.clone(),
                    resolve_device_id(&config),
                    Arc::clone(&registry),
                    ws_router_tx.clone(),
                    handle.clone(),
                );
                tokio::spawn(bridge.run());
                Some(handle)
            } else {
                warn!("role: client without a bridge block — running host-local only");
                None
            }
        } else {
            None
        };
        tokio::spawn(MessageRouter::run_with_context(
            router_rx,
            Arc::clone(&registry),
            Arc::clone(&event_bus),
            jwt_validator.clone(),
            kernel_start,
            config_path,
            event_store.clone(),
            mac_secret,
            Some(config_permissions),
            config.ipc_rate_limit_rps,
            config.action_caller_rate_limit_rps,
            config.action_caller_max_concurrent,
            config.action_timeout_ms,
            config.max_conn_errors,
            config.max_tracked_error_conns,
            config.session_idle_timeout_secs,
            config.prune_interval_secs,
            device_store.clone(),
            bridge_handle,
        ));

        // disconnect handler: unregister plugin + publish system.plugin_left
        let disc_registry = Arc::clone(&registry);
        let disc_bus = Arc::clone(&event_bus);
        tokio::spawn(Self::disconnect_loop(
            disconnect_rx,
            disc_registry,
            disc_bus,
        ));

        // WS disconnect handler (same logic, separate channel)
        let ws_disc_registry = Arc::clone(&registry);
        let ws_disc_bus = Arc::clone(&event_bus);
        tokio::spawn(Self::disconnect_loop(
            ws_disconnect_rx,
            ws_disc_registry,
            ws_disc_bus,
        ));

        // at-least-once delivery retry worker
        if let Some(store) = event_store {
            let retry_bus = Arc::clone(&event_bus);
            let retry_reg = Arc::clone(&registry);
            tokio::spawn(run_retry_worker(
                store,
                retry_bus,
                retry_reg,
                config.event_max_retries,
                config.event_retention_secs,
            ));
        }

        let mut supervisor = PluginSupervisor::with_events(
            &config.socket_path,
            config.log_buffer_lines,
            Some(Arc::clone(&event_bus)),
            Some(Arc::clone(&registry)),
            config.restart_backoff_base_ms,
            config.restart_backoff_max_ms,
        );
        supervisor.set_data_dir(config.data_dir.clone());
        let supervisor = Arc::new(supervisor);
        let sup_loop = Arc::clone(&supervisor);
        tokio::spawn(async move { sup_loop.monitor_loop().await });

        let watchdog_sup = Arc::clone(&supervisor);
        let watchdog_reg = Arc::clone(&registry);
        let watchdog_interval = std::time::Duration::from_secs(config.watchdog_interval_secs);
        let watchdog_timeout = std::time::Duration::from_secs(config.watchdog_timeout_secs);
        tokio::spawn(async move {
            watchdog_sup
                .watchdog_loop(watchdog_reg, watchdog_interval, watchdog_timeout)
                .await
        });

        let shutdown_supervisor = Arc::clone(&supervisor);
        let manager = Arc::new(PluginManager::new(supervisor, Arc::clone(&registry)));
        PluginLoader::load_all(&config.plugins, &manager, Some(&event_bus)).await;
        let api = ApiServer::new(
            config.port,
            bind_ip,
            manager,
            jwt_validator.clone(),
            device_store,
            Some(ws_router_tx),
            Some(ws_disconnect_tx),
            kernel_start,
            config.api_rate_limit_rps,
            config.api_rate_limit_burst,
            tls_cert_path,
            tls_key_path,
            config.plugins.clone(),
            config.ws_handshake_timeout_secs,
            config.max_ws_connections,
            config.ws_register_timeout_secs,
        );
        tokio::spawn(async move {
            if let Err(e) = api.run().await {
                error!("HTTP API error: {e}");
            }
        });

        info!("kernel ready");
        shutdown.await;
        info!("shutdown signal received");

        Self::graceful_shutdown(
            &registry,
            &shutdown_supervisor,
            config.default_grace_seconds,
        )
        .await;
        Ok(())
    }
}
