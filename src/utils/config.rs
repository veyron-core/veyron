use serde::Deserialize;
use std::path::{Path, PathBuf};
use tracing::warn;

/// One plugin entry in the `plugins:` list in config.yaml.
#[derive(Debug, Deserialize, Clone, Default)]
pub struct PluginDef {
    pub id: String,
    pub binary: String,
    #[serde(default = "default_restart_policy")]
    pub restart: String,
    #[serde(default = "default_max_restarts")]
    pub max_restarts: u32,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: Vec<String>,
    #[serde(default)]
    pub sandbox: bool,
    /// Seconds to wait after SIGTERM before SIGKILL on shutdown. 0 = default (5s).
    #[serde(default)]
    pub grace_seconds: u32,
    /// RLIMIT_NPROC cap. Unset = `runner::DEFAULT_MAX_PROCS`. Applied to
    /// every plugin, not just sandboxed ones.
    #[serde(default)]
    pub max_procs: Option<u64>,
    /// RLIMIT_AS cap in MiB. Unset = `runner::DEFAULT_MAX_VMEM_MB` (2048).
    /// 0 = unlimited (RLIM_INFINITY). Applied to every plugin, not just
    /// sandboxed ones.
    #[serde(default)]
    pub max_vmem_mb: Option<u64>,
    /// Permissions granted to this plugin by the operator. Any permission declared
    /// in plugin.json but absent here causes the kernel to refuse to load the plugin.
    /// Empty list means "allow all declared permissions" (no restriction).
    #[serde(default)]
    pub permissions: Vec<String>,
    /// Landlock filesystem ceiling for sandboxed plugins (R9-03): `full`
    /// (no restriction, default), `read-only`, or `none`. Only enforced when
    /// `sandbox: true` and the kernel supports Landlock.
    #[serde(default)]
    pub max_fs_access: Option<String>,
    /// Read-only paths granted to a restricted plugin (besides its own binary
    /// dir and system library dirs). Honored when `max_fs_access: read-only`.
    #[serde(default)]
    pub readonly_paths: Vec<PathBuf>,
    /// Writable paths granted to a restricted plugin. Honored when
    /// `max_fs_access` is `read-only` or `none`.
    #[serde(default)]
    pub writable_paths: Vec<PathBuf>,
}

fn default_restart_policy() -> String {
    "on-failure".to_string()
}
fn default_max_restarts() -> u32 {
    5
}

/// Kernel role (D-06). `Client` turns this kernel into a remote device: it
/// mirrors configured plugins to a host kernel over WebSocket, where they
/// register as `device.<cap>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    #[default]
    Host,
    Client,
}

/// Bridge settings for `role: client`. The client kernel connects to the host
/// kernel's WS gateway (`host_url` + `/ws`), registers each mirrored capability
/// as `device.<cap>`, and relays traffic between its local plugins and the host.
/// `token` is the JWT the client presents (host gateway auth); `secret` is the
/// host kernel's `jwt_secret` value, needed to derive the per-session frame MAC
/// key (the token alone cannot — the nonce is derived from the shared secret).
#[derive(Debug, Deserialize, Clone, Default)]
pub struct BridgeConfig {
    /// Host kernel base URL (`http://`/`https://` or full `ws://`/`wss://`).
    /// A base URL gets `/ws` appended; a ws(s) URL is used verbatim.
    pub host_url: String,
    /// JWT for the host's WS gateway. Optional when the host runs
    /// `allow_no_auth: true`.
    #[serde(default)]
    pub token: Option<String>,
    /// Host kernel's `jwt_secret`. Required on a secured host.
    #[serde(default)]
    pub secret: Option<String>,
    /// Local plugin ids to mirror to the host (each becomes `device.<cap>`).
    /// Must be a subset of `plugins`.
    #[serde(default)]
    pub mirror: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_pid_path")]
    pub pid_file: PathBuf,
    #[serde(default = "default_log_path")]
    pub log_file: PathBuf,
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default = "default_socket_path")]
    pub socket_path: String,
    #[serde(default)]
    pub jwt_secret: Option<String>,
    /// Explicit opt-out: allow the kernel to start with no JWT auth. Insecure —
    /// any local process can register as any plugin. Must be set deliberately.
    #[serde(default)]
    pub allow_no_auth: bool,
    /// Kernel role (D-06). Host by default; `client` enables the bridge.
    #[serde(default)]
    pub role: Role,
    /// Bridge settings, required when `role: client`.
    #[serde(default)]
    pub bridge: Option<BridgeConfig>,
    /// Stable device identifier advertised to the host. Defaults to `$HOSTNAME`.
    #[serde(default)]
    pub device_id: Option<String>,
    /// Plugins to auto-spawn on kernel start. Empty by default.
    #[serde(default)]
    pub plugins: Vec<PluginDef>,
    /// Directory of per-plugin drop-in config files (R10-01). Each `*.yaml`
    /// file carries exactly one plugin entry; they are globbed and merged
    /// into `plugins` in filename-sort order, after any inline `plugins:`
    /// entries. Default: `<config dir>/plugins.d/`. The inline list stays
    /// supported (deprecated) so existing configs keep booting.
    #[serde(default)]
    pub plugins_dir: Option<PathBuf>,
    #[serde(default = "default_watchdog_interval")]
    pub watchdog_interval_secs: u64,
    #[serde(default = "default_watchdog_timeout")]
    pub watchdog_timeout_secs: u64,
    #[serde(default = "default_log_buffer_lines")]
    pub log_buffer_lines: usize,
    /// Maximum concurrent UDS plugin connections. Excess connections are dropped immediately.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// HTTP API rate limit: sustained requests per second per JWT token. Default 100.
    #[serde(default)]
    pub api_rate_limit_rps: Option<u32>,
    /// HTTP API rate limit: burst allowance per JWT token on top of sustained rate. Default 20.
    #[serde(default)]
    pub api_rate_limit_burst: Option<u32>,
    /// Per-plugin IPC send rate limit (messages per second per connection). None = unlimited.
    /// Exceeding the limit sends ERR_RATE_LIMITED without disconnecting the plugin.
    #[serde(default)]
    pub ipc_rate_limit_rps: Option<u32>,
    /// R6-03: per-(caller, provider) action rate limit — requests/second one calling
    /// plugin may send through one action provider. None = unlimited. Exceeding sends
    /// ActionResponse{status: ACTION_QUOTA_EXCEEDED} without forwarding to the provider.
    #[serde(default)]
    pub action_caller_rate_limit_rps: Option<u32>,
    /// R6-03: per-(caller, provider) max simultaneous pending actions one calling
    /// plugin may have in flight against one action provider. None = unlimited.
    #[serde(default)]
    pub action_caller_max_concurrent: Option<u32>,
    /// R6-04: seconds of inactivity (no chunk traffic) after which an
    /// *accepted* streaming session is force-terminated with
    /// ActionStreamAbort{reason: "idle timeout"} to both sides. None =
    /// disabled (accepted sessions may live indefinitely), matching R6-03's
    /// "unset = unlimited" convention. Only applies post-acceptance — the
    /// accept/reject window is still governed by ActionRequest.timeout_ms.
    #[serde(default)]
    pub session_idle_timeout_secs: Option<u32>,
    /// Interval (seconds) between the router's periodic prune sweep (evict
    /// idle rate-limit keys, sweep expired actions, sweep idle sessions).
    /// Default: 60s in production. Tests set this to 2–5s for fast timeout
    /// coverage.
    #[serde(default = "default_prune_interval_secs")]
    pub prune_interval_secs: u64,
    /// TLS for the network path — **on by default** (D-07). When no
    /// cert/key are configured the kernel generates a self-signed pair on
    /// first start; set `tls: false` to serve plain HTTP (insecure).
    #[serde(default = "default_tls")]
    pub tls: bool,
    /// TLS certificate (PEM). Pair with `tls_key_path`; one without the other
    /// is a boot error (see `resolve_tls_paths`).
    #[serde(default)]
    pub tls_cert_path: Option<PathBuf>,
    /// TLS private key (PEM).
    #[serde(default)]
    pub tls_key_path: Option<PathBuf>,
    /// Audience the kernel requires in every accepted JWT (D-07). Unset =
    /// any `aud` accepted (pre-D-07 behaviour). `vyn token mint` defaults
    /// its `--aud` to this value.
    #[serde(default)]
    pub jwt_audience: Option<String>,
    /// Explicit bind address override (D-07). Default: `0.0.0.0` when
    /// `role: host` and auth is configured (remote devices can reach it),
    /// else `127.0.0.1`.
    #[serde(default)]
    pub bind: Option<String>,
    /// Seconds a JWT-authenticated WS connection may stay unregistered
    /// before the gateway drops it (D-07, closes the "never registers → no
    /// frame-MAC" gap). Registering arms the session MAC key.
    #[serde(default = "default_ws_register_timeout_secs")]
    pub ws_register_timeout_secs: u64,
    /// Override the plugin registry URL. Default: official vynkor-core/vynkor-plugins registry.
    /// Set to a private registry URL for air-gapped or enterprise deployments.
    #[serde(default)]
    pub registry_url: Option<String>,
    /// Ed25519 public key (hex, 32 bytes) used to verify `registry.json`
    /// entry signatures (T-11). Defaults to the built-in pinned maintainer
    /// key; set only when pairing with a private `registry_url` signed by a
    /// different key.
    #[serde(default)]
    pub marketplace_public_key: Option<String>,
    /// How long the registry cache (`~/.cache/vyn/registry.json`) is considered
    /// fresh before `plugin list --refresh` re-fetches it. Default: 3600 (1h).
    #[serde(default = "default_registry_cache_ttl_secs")]
    pub registry_cache_ttl_secs: u64,
    /// Base directory for scratch/cache files (marketplace registry cache, plugin
    /// install staging). Defaults to a per-user private dir — never the shared
    /// `/tmp` (AUDIT M-09) — via `XDG_RUNTIME_DIR`/`/run/user/<uid>`/`~/.local/state/vyn/run`.
    #[serde(default = "default_tmp_dir")]
    pub tmp_dir: PathBuf,
    /// Default action timeout (ms) when a plugin's `ActionRequest.timeout_ms` is 0.
    #[serde(default = "default_action_timeout_ms")]
    pub action_timeout_ms: u32,
    /// Base delay (ms) for exponential plugin restart backoff: `base * 2^restart_count`.
    #[serde(default = "default_restart_backoff_base_ms")]
    pub restart_backoff_base_ms: u64,
    /// Ceiling (ms) for exponential plugin restart backoff.
    #[serde(default = "default_restart_backoff_max_ms")]
    pub restart_backoff_max_ms: u64,
    /// Seconds to wait after SIGTERM before SIGKILL when a plugin doesn't set its
    /// own `grace_seconds`. Overridable per-plugin via `plugins[].grace_seconds`.
    #[serde(default = "default_grace_seconds")]
    pub default_grace_seconds: u32,
    /// Max delivery attempts for an event before the event bus marks it dead.
    #[serde(default = "default_event_max_retries")]
    pub event_max_retries: u32,
    /// Seconds terminal (delivered/dead) events are kept in `events.db` before
    /// being pruned.
    #[serde(default = "default_event_retention_secs")]
    pub event_retention_secs: u64,
    /// Capacity of the kernel's inbound IPC message channel (backpressure bound).
    #[serde(default = "default_router_channel_capacity")]
    pub router_channel_capacity: usize,
    /// Seconds an incomplete fragmented message is retained before being
    /// discarded (fragment-reassembly memory bound).
    #[serde(default = "default_fragment_timeout_secs")]
    pub fragment_timeout_secs: u64,
    /// Max concurrent in-flight fragment-reassembly streams per connection.
    #[serde(default = "default_max_reassembly_streams")]
    pub max_reassembly_streams: usize,
    /// Consecutive protocol errors from one connection before it is throttled.
    #[serde(default = "default_max_conn_errors")]
    pub max_conn_errors: u32,
    /// Cap on the per-connection error-budget map before idle entries are pruned.
    #[serde(default = "default_max_tracked_error_conns")]
    pub max_tracked_error_conns: usize,
    /// Seconds to wait for a WebSocket upgrade handshake before returning 408.
    #[serde(default = "default_ws_handshake_timeout_secs")]
    pub ws_handshake_timeout_secs: u64,
    /// Maximum concurrent WebSocket gateway connections. Excess upgrade requests
    /// are rejected with 503 before the handshake completes (T-09; mirrors
    /// `max_connections` for the UDS listener).
    #[serde(default = "default_max_ws_connections")]
    pub max_ws_connections: usize,
    /// Max size (bytes) of a downloaded plugin archive before install is aborted.
    #[serde(default = "default_max_archive_bytes")]
    pub max_archive_bytes: u64,
    /// Max total decompressed size (bytes) an installed archive may extract to.
    #[serde(default = "default_max_extracted_bytes")]
    pub max_extracted_bytes: u64,
    /// Max number of entries an installed archive may contain.
    #[serde(default = "default_max_archive_entries")]
    pub max_archive_entries: usize,
    /// Path this config was loaded from — used by reload_config kernel command.
    #[serde(skip)]
    pub config_file: Option<String>,
}

/// Public so SDKs resolve the same default as the kernel when
/// `VYN_SOCKET_PATH` is not set.
pub use vynkor_wire::socket::default_socket_path;

fn default_port() -> u16 {
    8000
}
fn default_log_level() -> String {
    "info".to_string()
}

/// pid/log files got the same symlink-attack surface as the socket
/// (AUDIT M-09) — default them out of the shared `/tmp` the same way.
/// see ipc/server.rs for the socket-perms rationale.
fn default_pid_path() -> PathBuf {
    vynkor_wire::socket::default_private_dir()
        .map(|dir| dir.join("vyn.pid"))
        .unwrap_or_else(|| PathBuf::from("vyn.pid"))
}

fn default_log_path() -> PathBuf {
    vynkor_wire::socket::default_private_dir()
        .map(|dir| dir.join("vyn.log"))
        .unwrap_or_else(|| PathBuf::from("vyn.log"))
}

/// Per-user private data dir for the event store and per-plugin state.
/// S2: the events DB must never live in world-writable `/tmp` — a local user
/// could pre-create the path and forge pending events.
fn default_data_dir() -> PathBuf {
    vynkor_wire::socket::default_private_dir()
        .map(|dir| dir.join("vyn-data"))
        .unwrap_or_else(|| PathBuf::from("vyn-data"))
}

fn default_watchdog_interval() -> u64 {
    30
}
fn default_watchdog_timeout() -> u64 {
    10
}
fn default_log_buffer_lines() -> usize {
    1000
}
fn default_max_connections() -> usize {
    1024
}
fn default_prune_interval_secs() -> u64 {
    60
}
fn default_registry_cache_ttl_secs() -> u64 {
    3600
}
/// Per-user private scratch dir — mirrors `default_pid_path`/`default_log_path`'s
/// refusal to fall back into the shared, world-writable `/tmp` (AUDIT M-09).
fn default_tmp_dir() -> PathBuf {
    vynkor_wire::socket::default_private_dir().unwrap_or_else(std::env::temp_dir)
}
fn default_action_timeout_ms() -> u32 {
    30_000
}
fn default_restart_backoff_base_ms() -> u64 {
    100
}
fn default_restart_backoff_max_ms() -> u64 {
    30_000
}
fn default_grace_seconds() -> u32 {
    5
}
fn default_event_max_retries() -> u32 {
    5
}
fn default_event_retention_secs() -> u64 {
    3600
}
fn default_router_channel_capacity() -> usize {
    1024
}
fn default_fragment_timeout_secs() -> u64 {
    30
}
fn default_max_reassembly_streams() -> usize {
    64
}
fn default_max_conn_errors() -> u32 {
    16
}
fn default_max_tracked_error_conns() -> usize {
    8192
}
fn default_ws_handshake_timeout_secs() -> u64 {
    5
}
fn default_ws_register_timeout_secs() -> u64 {
    10
}
fn default_tls() -> bool {
    true
}
fn default_max_ws_connections() -> usize {
    1024
}
fn default_max_archive_bytes() -> u64 {
    256 * 1024 * 1024
}
fn default_max_extracted_bytes() -> u64 {
    1024 * 1024 * 1024
}
fn default_max_archive_entries() -> usize {
    10_000
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: default_port(),
            log_level: default_log_level(),
            pid_file: default_pid_path(),
            log_file: default_log_path(),
            data_dir: default_data_dir(),
            socket_path: default_socket_path(),
            jwt_secret: None,
            allow_no_auth: false,
            role: Role::Host,
            bridge: None,
            device_id: None,
            plugins: vec![],
            plugins_dir: None,
            watchdog_interval_secs: default_watchdog_interval(),
            watchdog_timeout_secs: default_watchdog_timeout(),
            log_buffer_lines: default_log_buffer_lines(),
            max_connections: default_max_connections(),
            api_rate_limit_rps: None,
            api_rate_limit_burst: None,
            ipc_rate_limit_rps: None,
            action_caller_rate_limit_rps: None,
            action_caller_max_concurrent: None,
            session_idle_timeout_secs: None,
            prune_interval_secs: default_prune_interval_secs(),
            tls: default_tls(),
            tls_cert_path: None,
            tls_key_path: None,
            jwt_audience: None,
            bind: None,
            ws_register_timeout_secs: default_ws_register_timeout_secs(),
            registry_url: None,
            marketplace_public_key: None,
            registry_cache_ttl_secs: default_registry_cache_ttl_secs(),
            tmp_dir: default_tmp_dir(),
            action_timeout_ms: default_action_timeout_ms(),
            restart_backoff_base_ms: default_restart_backoff_base_ms(),
            restart_backoff_max_ms: default_restart_backoff_max_ms(),
            default_grace_seconds: default_grace_seconds(),
            event_max_retries: default_event_max_retries(),
            event_retention_secs: default_event_retention_secs(),
            router_channel_capacity: default_router_channel_capacity(),
            fragment_timeout_secs: default_fragment_timeout_secs(),
            max_reassembly_streams: default_max_reassembly_streams(),
            max_conn_errors: default_max_conn_errors(),
            max_tracked_error_conns: default_max_tracked_error_conns(),
            ws_handshake_timeout_secs: default_ws_handshake_timeout_secs(),
            max_ws_connections: default_max_ws_connections(),
            max_archive_bytes: default_max_archive_bytes(),
            max_extracted_bytes: default_max_extracted_bytes(),
            max_archive_entries: default_max_archive_entries(),
            config_file: None,
        }
    }
}

pub fn load_config(path: &str) -> anyhow::Result<Config> {
    let content = std::fs::read_to_string(path)?;
    let mut config: Config = serde_yaml::from_str(&content)?;
    config.config_file = Some(path.to_string());
    config.plugins_dir = Some(resolve_plugins_dir(path, config.plugins_dir.as_deref()));
    merge_plugin_dropins(&mut config)?;
    clamp_invalid_numerics(&mut config);
    Ok(config)
}

/// Resolve the drop-in plugin dir: an explicit `plugins_dir` config key wins,
/// otherwise `<config dir>/plugins.d`. Shared by `load_config` (boot + SIGHUP)
/// and the CLI (`vyn plugin install` must write to the same place the kernel
/// reads from).
pub fn resolve_plugins_dir(config_path: &str, explicit: Option<&Path>) -> PathBuf {
    explicit.map(Path::to_path_buf).unwrap_or_else(|| {
        Path::new(config_path)
            .parent()
            .map(|p| p.join("plugins.d"))
            .unwrap_or_else(|| PathBuf::from("plugins.d"))
    })
}

/// Glob `plugins_dir/*.yaml`, parse each as one `PluginDef`, and append to
/// `config.plugins` in filename-sort order. A duplicate `id` — across drop-in
/// files or clashing with an inline `plugins:` entry — is a boot error: the
/// operator must not silently get one of the two definitions ignored.
fn merge_plugin_dropins(config: &mut Config) -> anyhow::Result<()> {
    let dir = config
        .plugins_dir
        .as_ref()
        .expect("plugins_dir resolved by load_config");
    if !dir.is_dir() {
        return Ok(()); // no drop-ins yet — nothing to merge
    }
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "yaml" || e == "yml"))
        .collect();
    files.sort(); // filename sort = deterministic merge order (R10-01)
    for file in files {
        let content = std::fs::read_to_string(&file)?;
        let plugin: PluginDef = serde_yaml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("{}: {}", file.display(), e))?;
        if config.plugins.iter().any(|p| p.id == plugin.id) {
            anyhow::bail!(
                "duplicate plugin id '{}': defined both in {} and in an inline `plugins:` entry or another drop-in file",
                plugin.id,
                file.display()
            );
        }
        config.plugins.push(plugin);
    }
    Ok(())
}

/// Zero is never a valid value for these fields — a hand-edited config that
/// ships `0` (or a legacy config that relied on "0 = default") would otherwise
/// silently disable the IPC channel / watchdog. Clamp to the defaults with a
/// warning instead of failing the whole load (N3).
fn clamp_invalid_numerics(config: &mut Config) {
    if config.router_channel_capacity == 0 {
        let d = default_router_channel_capacity();
        warn!("router_channel_capacity: 0 is invalid, clamping to default ({d})");
        config.router_channel_capacity = d;
    }
    if config.max_connections == 0 {
        let d = default_max_connections();
        warn!("max_connections: 0 is invalid, clamping to default ({d})");
        config.max_connections = d;
    }
    if config.watchdog_interval_secs == 0 {
        let d = default_watchdog_interval();
        warn!("watchdog_interval_secs: 0 is invalid, clamping to default ({d})");
        config.watchdog_interval_secs = d;
    }
    if config.watchdog_timeout_secs == 0 {
        let d = default_watchdog_timeout();
        warn!("watchdog_timeout_secs: 0 is invalid, clamping to default ({d})");
        config.watchdog_timeout_secs = d;
    }
    if config.max_ws_connections == 0 {
        let d = default_max_ws_connections();
        warn!("max_ws_connections: 0 is invalid, clamping to default ({d})");
        config.max_ws_connections = d;
    }
    if config.max_archive_bytes == 0 {
        let d = default_max_archive_bytes();
        warn!("max_archive_bytes: 0 is invalid, clamping to default ({d})");
        config.max_archive_bytes = d;
    }
}

/// Stable device identifier for the client role: explicit `device_id` config
/// wins, else `$HOSTNAME` (systemd sets it), else "unknown".
pub fn resolve_device_id(config: &Config) -> String {
    config
        .device_id
        .clone()
        .unwrap_or_else(|| std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string()))
}

/// Where the kernel auto-generates TLS material when none is configured
/// (D-07): `<private dir>/vyn-tls/` — same per-user private location as
/// the pid/log files (never the shared /tmp, AUDIT M-09).
pub fn default_tls_dir() -> Option<PathBuf> {
    vynkor_wire::socket::default_private_dir().map(|d| d.join("vyn-tls"))
}

/// The certificate a local `vyn` client must trust when TLS is on: the
/// operator-provided one, else the auto-generated self-signed cert.
pub fn effective_tls_cert_path(config: &Config) -> Option<PathBuf> {
    if !config.tls {
        return None;
    }
    if let Some(cert) = &config.tls_cert_path {
        return Some(cert.clone());
    }
    default_tls_dir().map(|d| d.join("cert.pem"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_socket_path_uses_xdg_runtime_dir() {
        temp_env::with_var("XDG_RUNTIME_DIR", Some("/run/user/1000"), || {
            let path = default_socket_path();
            assert_eq!(path, "/run/user/1000/vyn.sock");
        });
    }

    #[test]
    fn default_socket_path_never_falls_back_to_shared_tmp() {
        temp_env::with_var_unset("XDG_RUNTIME_DIR", || {
            let path = default_socket_path();
            assert_ne!(
                path, "/tmp/vyn.sock",
                "must not default into world-writable shared /tmp (BUG-006)"
            );
            // Must land in a per-user location: either the kernel-provided
            // /run/user/<uid>, or a private 0o700 dir under $HOME.
            assert!(
                path.starts_with("/run/user/") || path.contains("/.local/state/vyn/"),
                "expected a per-user private socket dir, got {path}"
            );
        });
    }

    #[test]
    fn default_pid_and_log_paths_never_land_in_shared_tmp() {
        temp_env::with_var("XDG_RUNTIME_DIR", Some("/run/user/1000"), || {
            let pid = default_pid_path();
            let log = default_log_path();

            assert_eq!(pid, PathBuf::from("/run/user/1000/vyn.pid"));
            assert_eq!(log, PathBuf::from("/run/user/1000/vyn.log"));
            assert!(
                !pid.starts_with("/tmp"),
                "pid file must not default into /tmp (AUDIT M-09)"
            );
            assert!(
                !log.starts_with("/tmp"),
                "log file must not default into /tmp (AUDIT M-09)"
            );
        });
    }

    #[test]
    fn default_data_dir_uses_xdg_runtime_dir() {
        temp_env::with_var("XDG_RUNTIME_DIR", Some("/run/user/1000"), || {
            let path = default_data_dir();
            assert_eq!(path, PathBuf::from("/run/user/1000/vyn-data"));
        });
    }

    #[test]
    fn default_data_dir_never_falls_back_to_shared_tmp() {
        temp_env::with_var_unset("XDG_RUNTIME_DIR", || {
            let path = default_data_dir();
            assert!(
                !path.starts_with("/tmp"),
                "data_dir must not default into world-writable /tmp (AUDIT S2)"
            );
            let path_str = path.to_string_lossy();
            assert!(
                path.starts_with("/run/user/") || path_str.contains("/.local/state/vyn/"),
                "expected a per-user private data dir, got {}",
                path.display()
            );
        });
    }

    fn write_minimal_config(dir: &tempfile::TempDir, extras: &str) -> String {
        let path = dir.path().join("config.yaml");
        let yaml = format!("port: 8000\nlog_level: info\ndata_dir: /tmp/vynkor-test\n{extras}");
        std::fs::write(&path, yaml).unwrap();
        path.display().to_string()
    }

    // Zero would silently disable the IPC channel / watchdog — load_config must
    // clamp it back to the documented defaults (N3).
    #[test]
    fn load_config_clamps_zero_numerics_to_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_minimal_config(
            &dir,
            "router_channel_capacity: 0\n\
             max_connections: 0\n\
             watchdog_interval_secs: 0\n\
             watchdog_timeout_secs: 0\n\
             max_ws_connections: 0\n\
             max_archive_bytes: 0\n",
        );
        let config = load_config(&path).unwrap();
        assert_eq!(
            config.router_channel_capacity,
            default_router_channel_capacity()
        );
        assert_eq!(config.max_connections, default_max_connections());
        assert_eq!(config.watchdog_interval_secs, default_watchdog_interval());
        assert_eq!(config.watchdog_timeout_secs, default_watchdog_timeout());
        assert_eq!(config.max_ws_connections, default_max_ws_connections());
        assert_eq!(config.max_archive_bytes, default_max_archive_bytes());
    }

    // Sane non-zero values must pass through untouched.
    #[test]
    fn load_config_preserves_sane_numerics() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_minimal_config(
            &dir,
            "router_channel_capacity: 2048\n\
             max_connections: 4096\n\
             watchdog_interval_secs: 60\n\
             watchdog_timeout_secs: 15\n",
        );
        let config = load_config(&path).unwrap();
        assert_eq!(config.router_channel_capacity, 2048);
        assert_eq!(config.max_connections, 4096);
        assert_eq!(config.watchdog_interval_secs, 60);
        assert_eq!(config.watchdog_timeout_secs, 15);
    }

    fn write_dropin(dir: &std::path::Path, filename: &str, id: &str) {
        std::fs::create_dir_all(dir.join("plugins.d")).unwrap();
        std::fs::write(
            dir.join("plugins.d").join(filename),
            format!("id: {id}\nbinary: /x/{id}\nrestart: on-failure\n"),
        )
        .unwrap();
    }

    // drop-in files merge into plugins in filename-sort order, after the
    // inline `plugins:` list
    #[test]
    fn load_config_merges_dropin_files_in_filename_order() {
        let dir = tempfile::tempdir().unwrap();
        write_dropin(dir.path(), "b.yaml", "bravo");
        write_dropin(dir.path(), "a.yaml", "alpha");
        let path = write_minimal_config(&dir, "plugins:\n  - id: inline\n    binary: /x/inline\n");
        let config = load_config(&path).unwrap();
        let ids: Vec<_> = config.plugins.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(ids, ["inline", "alpha", "bravo"]);
    }

    // a disabled drop-in (<slug>.yaml.disabled, R10-04) is not globbed — the
    // plugin stays installed but is skipped at boot and on SIGHUP reload
    #[test]
    fn load_config_skips_disabled_dropin() {
        let dir = tempfile::tempdir().unwrap();
        write_dropin(dir.path(), "a.yaml", "alpha");
        let path = write_minimal_config(&dir, "");

        let config = load_config(&path).unwrap();
        assert_eq!(config.plugins.len(), 1);

        std::fs::rename(
            dir.path().join("plugins.d/a.yaml"),
            dir.path().join("plugins.d/a.yaml.disabled"),
        )
        .unwrap();
        let config = load_config(&path).unwrap();
        assert!(
            config.plugins.is_empty(),
            "disabled drop-in must not merge into plugins"
        );
    }

    // default plugins_dir resolves to <config dir>/plugins.d
    #[test]
    fn load_config_default_plugins_dir_is_config_dir_plugins_d() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_minimal_config(&dir, "");
        let config = load_config(&path).unwrap();
        assert_eq!(config.plugins_dir.unwrap(), dir.path().join("plugins.d"));
    }

    // explicit plugins_dir config key wins over the default
    #[test]
    fn load_config_honors_explicit_plugins_dir() {
        let dir = tempfile::tempdir().unwrap();
        let custom = tempfile::tempdir().unwrap();
        std::fs::write(
            custom.path().join("x.yaml"),
            "id: custom\nbinary: /x/custom\nrestart: on-failure\n",
        )
        .unwrap();
        let path =
            write_minimal_config(&dir, &format!("plugins_dir: {}\n", custom.path().display()));
        let config = load_config(&path).unwrap();
        assert_eq!(config.plugins_dir.unwrap(), custom.path());
        assert_eq!(config.plugins.len(), 1);
        assert_eq!(config.plugins[0].id, "custom");
    }

    // duplicate id between a drop-in and the inline list fails boot loudly
    #[test]
    fn load_config_duplicate_id_across_inline_and_dropin_errors() {
        let dir = tempfile::tempdir().unwrap();
        write_dropin(dir.path(), "a.yaml", "dup");
        let path = write_minimal_config(&dir, "plugins:\n  - id: dup\n    binary: /x/dup\n");
        let err = load_config(&path).unwrap_err().to_string();
        assert!(err.contains("duplicate plugin id 'dup'"), "got: {err}");
    }

    // duplicate id across two drop-in files fails boot loudly
    #[test]
    fn load_config_duplicate_id_across_dropin_files_errors() {
        let dir = tempfile::tempdir().unwrap();
        write_dropin(dir.path(), "a.yaml", "dup");
        write_dropin(dir.path(), "b.yaml", "dup");
        let path = write_minimal_config(&dir, "");
        let err = load_config(&path).unwrap_err().to_string();
        assert!(err.contains("duplicate plugin id 'dup'"), "got: {err}");
    }

    // missing plugins.d dir is not an error — fresh installs have no drop-ins
    #[test]
    fn load_config_missing_plugins_dir_is_fine() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_minimal_config(&dir, "");
        let config = load_config(&path).unwrap();
        assert!(config.plugins.is_empty());
    }

    // all PluginDef fields survive the drop-in round-trip — a drop-in file is
    // not a reduced schema, it is the full per-plugin config surface
    #[test]
    fn load_config_dropin_roundtrips_full_plugindef() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("plugins.d")).unwrap();
        std::fs::write(
            dir.path().join("plugins.d").join("full.yaml"),
            "id: full\n\
             binary: /x/full\n\
             restart: always\n\
             max_restarts: 9\n\
             args: [--flag]\n\
             env: [A=B, C=D]\n\
             sandbox: true\n\
             grace_seconds: 7\n\
             max_procs: 128\n\
             max_vmem_mb: 256\n\
             permissions: [storage]\n\
             max_fs_access: read-only\n\
             readonly_paths: [/usr/share/foo]\n\
             writable_paths: [/var/lib/vyn/foo]\n",
        )
        .unwrap();
        let path = write_minimal_config(&dir, "");

        let config = load_config(&path).unwrap();
        let p = &config.plugins[0];
        assert_eq!(p.id, "full");
        assert_eq!(p.restart, "always");
        assert_eq!(p.max_restarts, 9);
        assert_eq!(p.args, ["--flag"]);
        assert_eq!(p.env, ["A=B", "C=D"]);
        assert!(p.sandbox);
        assert_eq!(p.grace_seconds, 7);
        assert_eq!(p.max_procs, Some(128));
        assert_eq!(p.max_vmem_mb, Some(256));
        assert_eq!(p.permissions, ["storage"]);
        assert_eq!(p.max_fs_access.as_deref(), Some("read-only"));
        assert_eq!(p.readonly_paths, [PathBuf::from("/usr/share/foo")]);
        assert_eq!(p.writable_paths, [PathBuf::from("/var/lib/vyn/foo")]);
    }

    #[test]
    fn load_config_defaults_to_host_role() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_minimal_config(&dir, "");
        let config = load_config(&path).unwrap();
        assert_eq!(config.role, Role::Host);
        assert!(config.bridge.is_none());
    }

    #[test]
    fn load_config_parses_client_role_with_bridge() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_minimal_config(
            &dir,
            "role: client\ndevice_id: laptop-7\nbridge:\n  host_url: https://hub.example.com\n  token: abc.def.ghi\n  secret: host-jwt-secret\n  mirror: [stt, kairo]\n",
        );
        let config = load_config(&path).unwrap();
        assert_eq!(config.role, Role::Client);
        assert_eq!(config.device_id.as_deref(), Some("laptop-7"));
        let bridge = config.bridge.unwrap();
        assert_eq!(bridge.host_url, "https://hub.example.com");
        assert_eq!(bridge.token.as_deref(), Some("abc.def.ghi"));
        assert_eq!(bridge.secret.as_deref(), Some("host-jwt-secret"));
        assert_eq!(bridge.mirror, ["stt", "kairo"]);
    }

    #[test]
    fn load_config_rejects_unknown_role() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_minimal_config(&dir, "role: satellite\n");
        assert!(load_config(&path).is_err());
    }

    #[test]
    fn resolve_device_id_priority() {
        let mut config = Config {
            device_id: Some("named".into()),
            ..Default::default()
        };
        assert_eq!(resolve_device_id(&config), "named");

        config.device_id = None;
        temp_env::with_var("HOSTNAME", Some("box-1"), || {
            assert_eq!(resolve_device_id(&config), "box-1");
        });
        temp_env::with_var_unset("HOSTNAME", || {
            assert_eq!(resolve_device_id(&config), "unknown");
        });
    }

    // D-07: the network path is TLS by default — only an explicit tls: false
    // serves plain HTTP
    #[test]
    fn tls_defaults_to_on() {
        assert!(Config::default().tls, "tls must default to on (D-07)");

        let dir = tempfile::tempdir().unwrap();
        let path = write_minimal_config(&dir, "");
        assert!(load_config(&path).unwrap().tls);

        let path = write_minimal_config(&dir, "tls: false\n");
        assert!(!load_config(&path).unwrap().tls);
    }

    #[test]
    fn jwt_audience_bind_and_ws_register_timeout_parse() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_minimal_config(
            &dir,
            "tls: false\njwt_audience: myhub\nbind: 0.0.0.0\nws_register_timeout_secs: 5\n",
        );
        let config = load_config(&path).unwrap();
        assert_eq!(config.jwt_audience.as_deref(), Some("myhub"));
        assert_eq!(config.bind.as_deref(), Some("0.0.0.0"));
        assert_eq!(config.ws_register_timeout_secs, 5);
    }

    #[test]
    fn ws_register_timeout_defaults_to_10_secs() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_minimal_config(&dir, "tls: false\n");
        assert_eq!(load_config(&path).unwrap().ws_register_timeout_secs, 10);
    }

    #[test]
    fn effective_tls_cert_path_prefers_configured_over_generated() {
        let configured = Config {
            tls: true,
            tls_cert_path: Some(PathBuf::from("/etc/vyn/cert.pem")),
            ..Default::default()
        };
        assert_eq!(
            effective_tls_cert_path(&configured),
            Some(PathBuf::from("/etc/vyn/cert.pem"))
        );

        // tls off → no cert to trust
        let off = Config {
            tls: false,
            ..Default::default()
        };
        assert_eq!(effective_tls_cert_path(&off), None);

        // tls on, no cert configured → the auto-generated path under the
        // private dir (probed with a temp XDG_RUNTIME_DIR)
        let auto = Config {
            tls: true,
            ..Default::default()
        };
        let dir = tempfile::tempdir().unwrap();
        temp_env::with_var("XDG_RUNTIME_DIR", Some(dir.path()), || {
            assert_eq!(
                effective_tls_cert_path(&auto),
                Some(dir.path().join("vyn-tls/cert.pem"))
            );
        });
    }
}
