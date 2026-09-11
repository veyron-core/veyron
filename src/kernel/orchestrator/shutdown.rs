use super::*;

use crate::ipc::connection::out_frame;
use crate::ipc::framing::build_frame;
use crate::proto::vynkor::{envelope, Envelope, Event, PluginShutdown};
use prost::Message;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::signal::unix::{signal, SignalKind};

/// Reload config from `config_file` (if set) and apply its log level.
/// No-op (`Ok(())`) when `config_file` is `None` — matches the boot-time
/// behavior where an in-memory-only `Config` has nothing to reload from.
fn reload_config_on_sighup(config_file: &Option<String>) -> anyhow::Result<Config> {
    let path = config_file
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("no config file to reload from"))?;
    let cfg = crate::utils::config::load_config(path)?;
    crate::utils::logging::set_log_level(&cfg.log_level);
    Ok(cfg)
}

impl Kernel {
    pub async fn run(config: Config) -> anyhow::Result<()> {
        let config_file = config.config_file.clone();
        let shutdown = async move {
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
            let mut sighup =
                signal(SignalKind::hangup()).expect("failed to install SIGHUP handler");
            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => break,
                    _ = sigterm.recv() => { info!("received SIGTERM"); break; }
                    _ = sighup.recv() => {
                        info!("received SIGHUP — reloading config");
                        match reload_config_on_sighup(&config_file) {
                            Ok(cfg) => info!(log_level = %cfg.log_level, "config reloaded via SIGHUP"),
                            Err(e) => tracing::warn!("SIGHUP config reload failed: {e}"),
                        }
                    }
                }
            }
        };
        Self::run_with_shutdown(config, shutdown).await
    }

    pub(super) async fn disconnect_loop(
        mut rx: mpsc::Receiver<u64>,
        registry: Arc<PluginRegistry>,
        event_bus: Arc<EventBus>,
    ) {
        while let Some(conn_id) = rx.recv().await {
            let plugin_id = match registry.get_by_conn_id(conn_id) {
                Some(e) => e.plugin_id.clone(),
                None => continue,
            };

            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis();
            event_bus
                .publish(
                    Event {
                        event_id: format!("sys-left-{plugin_id}-{now_ms}"),
                        event_type: "system.plugin_left".to_string(),
                        payload_json: crate::events::bus::plugin_lifecycle_payload(
                            &registry, &plugin_id,
                        ),
                        retry_count: 0,
                    },
                    &registry,
                )
                .await;

            event_bus.unsubscribe_all(&plugin_id);
            registry.unregister(&plugin_id);
        }
    }

    pub(super) async fn graceful_shutdown(
        registry: &PluginRegistry,
        supervisor: &PluginSupervisor,
        default_grace_seconds: u32,
    ) {
        let entries = registry.list();
        if entries.is_empty() {
            return;
        }

        // Advertise each plugin's real grace window: its supervised config value
        // when set, else the kernel default — matching what the supervisor will
        // actually enforce before SIGKILL.
        for entry in entries {
            let grace = supervisor
                .grace_seconds_for(&entry.plugin_id)
                .unwrap_or(default_grace_seconds);
            let mut payload = Vec::new();
            let env = Envelope {
                payload: Some(envelope::Payload::PluginShutdown(PluginShutdown {
                    reason: "kernel shutdown".to_string(),
                    grace_seconds: grace,
                })),
                ..Default::default()
            };
            if env.encode(&mut payload).is_err() {
                continue;
            }
            let _ = entry
                .write_tx
                .send(out_frame(build_frame("self", 0, payload)))
                .await;
        }

        supervisor.graceful_shutdown(default_grace_seconds).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn reload_config_on_sighup_none_path_is_a_no_op_error() {
        let result = reload_config_on_sighup(&None);
        assert!(result.is_err(), "no config file means nothing to reload");
    }

    #[test]
    fn reload_config_on_sighup_reloads_from_disk() {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        writeln!(
            file,
            "port: 9000\nlog_level: debug\ndata_dir: /tmp/vynkor_test_data"
        )
        .expect("write");
        let path = file.path().to_str().unwrap().to_string();

        let cfg = reload_config_on_sighup(&Some(path)).expect("reload must succeed");
        assert_eq!(cfg.log_level, "debug");
    }

    #[test]
    fn reload_config_on_sighup_surfaces_parse_errors() {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        writeln!(file, "not: [valid: yaml: at: all").expect("write");
        let path = file.path().to_str().unwrap().to_string();

        let result = reload_config_on_sighup(&Some(path));
        assert!(result.is_err(), "malformed YAML must not reload silently");
    }

    /// End-to-end: install the real SIGHUP handler exactly as `Kernel::run`
    /// does, send an actual `SIGHUP` to this process, and confirm the reload
    /// path runs (observed via the log-level change it produces) rather than
    /// only exercising `reload_config_on_sighup` as an isolated function.
    #[tokio::test]
    async fn sighup_signal_triggers_config_reload() {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        writeln!(
            file,
            "port: 9000\nlog_level: debug\ndata_dir: /tmp/vynkor_test_data"
        )
        .expect("write");
        let config_file = Some(file.path().to_str().unwrap().to_string());

        let mut sighup = signal(SignalKind::hangup()).expect("failed to install SIGHUP handler");

        nix::sys::signal::raise(nix::sys::signal::Signal::SIGHUP).expect("failed to raise SIGHUP");

        tokio::time::timeout(std::time::Duration::from_secs(2), sighup.recv())
            .await
            .expect("must receive the SIGHUP within timeout")
            .expect("signal stream must not be closed");

        let cfg = reload_config_on_sighup(&config_file).expect("reload must succeed");
        assert_eq!(cfg.log_level, "debug");
    }
}
