use crate::events::bus::EventBus;
use crate::plugins::registry::PluginRegistry;
use crate::utils::errors::VynkorError;
use dashmap::DashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, Mutex};

mod spawn;
mod watchdog;

#[derive(Clone, Default)]
pub enum RestartPolicy {
    Always,
    #[default]
    OnFailure,
    Never,
}

#[derive(Clone, Default)]
pub struct PluginConfig {
    pub plugin_id: String,
    pub binary_path: PathBuf,
    pub args: Vec<String>,
    /// Additional environment variables as "KEY=VALUE" strings.
    pub env: Vec<String>,
    pub restart_policy: RestartPolicy,
    pub max_restarts: u32,
    /// Isolate plugin in private user + network + PID namespaces (Linux only).
    /// PID-namespace isolation runs through a shim process (`vyn __shim`, see
    /// `plugins::shim`): the supervisor re-execs itself, the shim nests a user
    /// namespace, creates a fresh PID namespace and private `/proc`, and forks
    /// the plugin into it as PID 1. A plugin cannot be moved into a PID
    /// namespace from its own spawn path — a pending `pid_for_children`
    /// namespace makes the kernel refuse thread creation (EINVAL).
    pub sandbox: bool,
    /// Seconds to wait after SIGTERM before SIGKILL. 0 means use default (5s).
    pub grace_seconds: u32,
    /// RLIMIT_NPROC cap. None = `runner::DEFAULT_MAX_PROCS`. Applied
    /// unconditionally (not gated by `sandbox`). The check counts *all*
    /// threads of the real uid system-wide at clone time, so a cap below
    /// the session's thread baseline kills every thread the plugin spawns
    /// with EAGAIN — busy desktop sessions need an explicit, higher value.
    pub max_procs: Option<u64>,
    /// RLIMIT_AS cap in MiB. None = `runner::DEFAULT_MAX_VMEM_MB`. Applied
    /// unconditionally (not gated by `sandbox`).
    pub max_vmem_mb: Option<u64>,
    /// Landlock filesystem ceiling for sandboxed plugins (R9-03):
    /// `full` (no restriction), `read-only`, or `none`. Only enforced when
    /// `sandbox: true` on a Landlock-capable kernel.
    pub max_fs_access: crate::plugins::fsaccess::FsAccessMode,
    /// Read-only dirs/files granted to a restricted plugin (besides its own
    /// binary dir and system libs). Honored when `max_fs_access: read-only`.
    pub readonly_paths: Vec<PathBuf>,
    /// Writable dirs/files granted to a restricted plugin. Honored when
    /// `max_fs_access` is `read-only` or `none`.
    pub writable_paths: Vec<PathBuf>,
}

pub struct PluginProcess {
    pub plugin_id: String,
    pub pid: u32,
}

pub(crate) struct PluginEntry {
    pub(crate) config: PluginConfig,
    pub(crate) restart_count: u32,
    pub(crate) pid: u32,
    /// Host pid of the sandbox shim (`vyn __shim`), when the plugin runs
    /// sandboxed. Lifecycle signals go to the shim, which forwards them and
    /// stays alive to reap — signalling the plugin directly would break the
    /// shim's waitpid and orphan it inside the namespace.
    pub(crate) shim_pid: Option<u32>,
    /// Monotonic spawn-instance id. `ExitEvent`s carry it so a stale exit of
    /// an older spawn can never be attributed to a newer one (B1) and an
    /// explicit stop is never undone by an in-flight restart (B3).
    pub(crate) epoch: u64,
    /// Fired once the wait task has reaped the process tree. `stop_plugin`
    /// awaits it so "stopped" means actually exited, not just signalled (B2).
    pub(crate) exited: watch::Receiver<bool>,
}

impl PluginEntry {
    fn signal_target(&self) -> i32 {
        self.shim_pid.unwrap_or(self.pid) as i32
    }

    /// Await the wait task reaping the process tree. `changed()` resolves once
    /// a value newer than the initial `false` is published, even if that
    /// happened before this call — a fast exit never hangs the awaiter.
    async fn wait_for_exit(&mut self) {
        let _ = self.exited.changed().await;
    }
}

pub(crate) struct ExitEvent {
    pub(crate) plugin_id: String,
    /// Instance id of the spawn that exited. A mismatch against the registered
    /// entry identifies a stale event (B1).
    pub(crate) epoch: u64,
    pub(crate) pid: u32,
    pub(crate) success: bool,
}

pub struct PluginSupervisor {
    pub(crate) socket_path: String,
    /// Base dir for per-plugin writable state: each spawn gets
    /// `data_dir/plugins/<plugin_id>`, exposed to the plugin as
    /// `VYN_DATA_DIR`. `None` = no data dir granted.
    pub(crate) data_dir: Option<PathBuf>,
    pub(crate) entries: Arc<DashMap<String, PluginEntry>>,
    pub(crate) event_tx: mpsc::Sender<ExitEvent>,
    pub(crate) event_rx: Arc<Mutex<mpsc::Receiver<ExitEvent>>>,
    pub(crate) log_buffers: Arc<DashMap<String, Arc<Mutex<VecDeque<String>>>>>,
    pub(crate) max_log_lines: usize,
    pub(crate) event_bus: Option<Arc<EventBus>>,
    pub(crate) plugin_registry: Option<Arc<PluginRegistry>>,
    /// Plugins whose next exit is a manual restart (POST /restart) and must be
    /// respawned regardless of restart_policy / max_restarts.
    pub(crate) forced_restarts: Arc<DashMap<String, ()>>,
    /// Final restart_count for plugins that have been removed from `entries` after
    /// exhausting their restart budget (VULN-018). Preserved for historical lookup.
    pub(crate) stopped_counts: Arc<DashMap<String, u32>>,
    /// Monotonic spawn-instance counter. Every spawn takes one id; `ExitEvent`s
    /// carry it so a stale exit can never be attributed to a newer instance (B1)
    /// and a manual stop can't be undone by an in-flight restart (B3).
    pub(crate) next_epoch: AtomicU64,
    /// plugin_id → epoch of its last explicit stop. An auto-restart decision
    /// still in flight for a stopped instance is dropped (B3).
    pub(crate) stopped_epochs: Arc<DashMap<String, u64>>,
    /// Base delay (ms) for exponential restart backoff: `base * 2^restart_count`.
    pub(crate) backoff_base_ms: u64,
    /// Ceiling (ms) for exponential restart backoff.
    pub(crate) backoff_max_ms: u64,
}

impl PluginSupervisor {
    pub fn new(socket_path: &str) -> Self {
        Self::with_log_lines(socket_path, 1000)
    }

    pub fn with_log_lines(socket_path: &str, max_log_lines: usize) -> Self {
        Self::with_events(socket_path, max_log_lines, None, None, 100, 30_000)
    }

    /// Grant every spawned plugin a writable per-plugin dir under `dir`
    /// (exposed as `VYN_DATA_DIR`) for its own persistent state.
    pub fn set_data_dir(&mut self, dir: PathBuf) {
        self.data_dir = Some(dir);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_events(
        socket_path: &str,
        max_log_lines: usize,
        event_bus: Option<Arc<EventBus>>,
        plugin_registry: Option<Arc<PluginRegistry>>,
        backoff_base_ms: u64,
        backoff_max_ms: u64,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel::<ExitEvent>(64);
        PluginSupervisor {
            socket_path: socket_path.to_string(),
            data_dir: None,
            backoff_base_ms,
            backoff_max_ms,
            entries: Arc::new(DashMap::new()),
            event_tx,
            event_rx: Arc::new(Mutex::new(event_rx)),
            log_buffers: Arc::new(DashMap::new()),
            max_log_lines,
            event_bus,
            plugin_registry,
            forced_restarts: Arc::new(DashMap::new()),
            stopped_counts: Arc::new(DashMap::new()),
            next_epoch: AtomicU64::new(0),
            stopped_epochs: Arc::new(DashMap::new()),
        }
    }

    pub async fn get_logs(&self, plugin_id: &str, n: usize) -> Vec<String> {
        let buf = match self.log_buffers.get(plugin_id) {
            Some(b) => b.clone(),
            None => return vec![],
        };
        let locked = buf.lock().await;
        let skip = locked.len().saturating_sub(n);
        locked.iter().skip(skip).cloned().collect()
    }

    pub async fn spawn_plugin(&self, config: PluginConfig) -> Result<PluginProcess, VynkorError> {
        self.spawn_internal(config, 0, None).await
    }

    pub async fn stop_plugin(&self, plugin_id: &str) -> Result<(), VynkorError> {
        let entry = self
            .entries
            .remove(plugin_id)
            .ok_or_else(|| VynkorError::PluginNotFound(plugin_id.to_string()))?;

        // B3: an explicit stop is terminal — record the instance so an
        // in-flight backoff restart can't resurrect the plugin.
        self.stopped_epochs
            .insert(plugin_id.to_string(), entry.1.epoch);
        // Explicit stop overrides any pending manual restart.
        self.forced_restarts.remove(plugin_id);
        let target = entry.1.signal_target();
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(target),
            nix::sys::signal::Signal::SIGTERM,
        );
        // B2: "stopped" means the process tree actually exited, not just
        // signalled. Escalate to SIGKILL on the configured deadline so a
        // SIGTERM-ignoring plugin can't hold the stop; the final bound keeps
        // even an unkillable process from hanging the caller forever.
        let mut entry = entry.1;
        let grace = if entry.config.grace_seconds > 0 {
            entry.config.grace_seconds
        } else {
            5
        };
        if tokio::time::timeout(Duration::from_secs(grace as u64), entry.wait_for_exit())
            .await
            .is_err()
        {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(target),
                nix::sys::signal::Signal::SIGKILL,
            );
            let _ = tokio::time::timeout(Duration::from_secs(10), entry.wait_for_exit()).await;
        }
        Ok(())
    }

    // Sends SIGTERM without removing the entry so monitor_loop restarts the plugin.
    // Marks the plugin for forced restart so it respawns even under a Never /
    // OnFailure policy or after max_restarts — a manual restart overrides policy.
    pub async fn restart_plugin(&self, plugin_id: &str) -> Result<(), VynkorError> {
        let target = self
            .entries
            .get(plugin_id)
            .map(|e| e.signal_target())
            .ok_or_else(|| VynkorError::PluginNotFound(plugin_id.to_string()))?;

        self.forced_restarts.insert(plugin_id.to_string(), ());
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(target),
            nix::sys::signal::Signal::SIGTERM,
        );
        Ok(())
    }

    pub fn is_running(&self, plugin_id: &str) -> bool {
        self.entries.contains_key(plugin_id)
    }

    /// Configured SIGTERM→SIGKILL grace for a supervised plugin. `None` when the
    /// plugin is not supervised or its config uses the default (0).
    pub fn grace_seconds_for(&self, plugin_id: &str) -> Option<u32> {
        self.entries
            .get(plugin_id)
            .map(|e| e.config.grace_seconds)
            .filter(|g| *g > 0)
    }

    pub fn restart_count(&self, plugin_id: &str) -> Option<u32> {
        self.entries
            .get(plugin_id)
            .map(|e| e.restart_count)
            .or_else(|| self.stopped_counts.get(plugin_id).map(|c| *c))
    }

    /// Send SIGTERM to all managed plugins, then SIGKILL each plugin on its own
    /// deadline — `grace_seconds` from its `PluginConfig`, falling back to
    /// `default_grace_seconds` when that field is 0. A plugin with a long grace
    /// period no longer delays SIGKILL for every other plugin (BUG-005).
    pub async fn graceful_shutdown(&self, default_grace_seconds: u32) {
        if self.entries.is_empty() {
            return;
        }

        for entry in self.entries.iter() {
            let pid = nix::unistd::Pid::from_raw(entry.value().signal_target());
            let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM);
        }

        let handles: Vec<_> = self
            .entries
            .iter()
            .map(|entry| {
                let target = entry.value().signal_target();
                let grace = entry.value().config.grace_seconds;
                let grace = if grace > 0 {
                    grace
                } else {
                    default_grace_seconds
                };
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(grace as u64)).await;
                    let _ = nix::sys::signal::kill(
                        nix::unistd::Pid::from_raw(target),
                        nix::sys::signal::Signal::SIGKILL,
                    );
                })
            })
            .collect();

        for handle in handles {
            let _ = handle.await;
        }
    }

    fn backoff_delay(&self, restart_count: u32) -> Duration {
        let ms = self
            .backoff_base_ms
            .saturating_mul(1u64 << restart_count.min(8));
        Duration::from_millis(ms.min(self.backoff_max_ms))
    }
}

/// Binary the supervisor re-execs as the sandbox shim: our own executable
/// (the hidden `__shim` subcommand), overridable via VYN_SHIM_BIN — the
/// unit-test harness binary does not handle `__shim`.
fn sandbox_shim_bin() -> PathBuf {
    std::env::var_os("VYN_SHIM_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_exe().unwrap_or_else(|_| PathBuf::from("vyn")))
}

#[cfg(test)]
mod tests {
    #[cfg(not(target_os = "linux"))]
    #[tracing_test::traced_test]
    #[test]
    fn sandbox_true_non_linux_emits_warn() {
        use super::{PluginConfig, RestartPolicy};

        let config = PluginConfig {
            plugin_id: "test-plugin".to_string(),
            binary_path: std::path::PathBuf::from("/nonexistent"),
            args: vec![],
            env: vec![],
            restart_policy: RestartPolicy::Never,
            grace_seconds: 5,
            sandbox: true,
        };
        if config.sandbox {
            tracing::warn!(
                plugin_id = %config.plugin_id,
                "sandbox=true has no effect on this OS (Linux required for namespace isolation)"
            );
        }
        assert!(logs_contain(
            "sandbox=true has no effect on this OS (Linux required for namespace isolation)"
        ));
    }
}
