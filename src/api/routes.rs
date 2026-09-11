use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;

use crate::auth::jwt::JwtValidator;
use crate::plugins::loader::{validate_plugin_def, PluginLoader};
use crate::plugins::manager::PluginManager;
use crate::utils::config::PluginDef;
use crate::utils::errors::VynkorError;

pub struct AppState {
    pub manager: Arc<PluginManager>,
    pub jwt_validator: Option<Arc<JwtValidator>>,
    pub started_at: Instant,
    /// Plugins declared under `plugins:` in config.yaml — the set `POST
    /// /plugins/:id/start` is allowed to spawn (never arbitrary binaries).
    pub plugin_defs: Vec<PluginDef>,
}

#[derive(Serialize)]
pub struct Health {
    pub status: &'static str,
    pub version: &'static str,
    pub uptime_secs: u64,
    pub plugins: usize,
}

/// Real health report: process liveness plus readiness detail (uptime, plugin
/// count, build version). Returns 200 whenever the process can answer.
pub async fn health_check(State(state): State<Arc<AppState>>) -> Json<Health> {
    Json(Health {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        uptime_secs: state.started_at.elapsed().as_secs(),
        plugins: state.manager.list().len(),
    })
}

#[derive(Serialize)]
pub struct PluginInfo {
    pub plugin_id: String,
    pub state: String,
    pub registered_at: u64,
    pub permissions: Vec<String>,
    /// owning device (D-02); "local" for host plugins
    pub device_id: String,
    /// last ping/pong of the owning device, unix millis (D-04)
    pub last_seen: u64,
}

pub async fn list_plugins(State(state): State<Arc<AppState>>) -> Json<Vec<PluginInfo>> {
    let plugins = state
        .manager
        .list()
        .into_iter()
        .map(|e| {
            let last_seen = state
                .manager
                .registry()
                .get_device(&e.device_id)
                .map(|d| d.last_seen)
                .unwrap_or(0);
            PluginInfo {
                plugin_id: e.plugin_id.clone(),
                state: crate::plugins::registry::plugin_state_str(&e.state).to_string(),
                registered_at: e.registered_at,
                permissions: e.manifest.permissions.clone(),
                device_id: e.device_id.clone(),
                last_seen,
            }
        })
        .collect();
    Json(plugins)
}

pub async fn get_plugin(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<PluginInfo>, StatusCode> {
    state
        .manager
        .get(&id)
        .map(|e| {
            let last_seen = state
                .manager
                .registry()
                .get_device(&e.device_id)
                .map(|d| d.last_seen)
                .unwrap_or(0);
            Json(PluginInfo {
                plugin_id: e.plugin_id.clone(),
                state: crate::plugins::registry::plugin_state_str(&e.state).to_string(),
                registered_at: e.registered_at,
                permissions: e.manifest.permissions.clone(),
                device_id: e.device_id.clone(),
                last_seen,
            })
        })
        .ok_or(StatusCode::NOT_FOUND)
}

#[derive(Serialize)]
pub struct DeviceInfoView {
    pub device_id: String,
    pub os: String,
    pub arch: String,
    pub os_version: String,
    pub capabilities: Vec<String>,
    /// unix millis of the last ping/pong from any plugin on the device
    pub last_seen: u64,
    pub state: String,
    /// E-01 (v1.7): pairing lifecycle, unix millis; 0 = unpaired/unknown
    pub created: u64,
    pub expires: u64,
}

// D-04: discovery surface — the registry's device map as a serializable view.
pub async fn list_devices(State(state): State<Arc<AppState>>) -> Json<Vec<DeviceInfoView>> {
    let devices = state.manager.registry().list_devices();
    Json(
        devices
            .into_iter()
            .map(|d| DeviceInfoView {
                device_id: d.device_id,
                os: crate::plugins::registry::device_os_str(d.os).to_string(),
                arch: d.arch,
                os_version: d.os_version,
                capabilities: d.capabilities,
                last_seen: d.last_seen,
                state: crate::plugins::registry::device_state_str(d.state).to_string(),
                created: d.created,
                expires: d.expires,
            })
            .collect(),
    )
}

/// Spawn a plugin declared under `plugins:` in config.yaml. 404 when `id`
/// isn't declared there (this never runs an arbitrary binary path);
/// 409 when it's already supervised.
pub async fn start_plugin(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> StatusCode {
    if state.manager.is_supervised(&id) {
        return StatusCode::CONFLICT;
    }
    let def = match state.plugin_defs.iter().find(|d| d.id == id) {
        Some(d) => d,
        None => return StatusCode::NOT_FOUND,
    };
    match validate_plugin_def(def) {
        Ok(_) => {}
        Err(VynkorError::PermissionDenied(_)) => return StatusCode::FORBIDDEN,
        Err(_) => return StatusCode::UNPROCESSABLE_ENTITY,
    }
    let config = PluginLoader::config_from_def(def);
    match state.manager.start(config).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::UNPROCESSABLE_ENTITY,
    }
}

/// Stop a supervised plugin and remove its registration (manager unregisters
/// regardless of the stop outcome). 404 when `id` has no supervised process —
/// including one that exited between the registry check and the stop.
pub async fn stop_plugin(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> StatusCode {
    if state.manager.get(&id).is_none() {
        return StatusCode::NOT_FOUND;
    }
    match state.manager.stop(&id).await {
        Ok(()) => StatusCode::OK,
        // ux-1: race with plugin exit is a miss, not a success
        Err(VynkorError::PluginNotFound(_)) => StatusCode::NOT_FOUND,
        Err(_) => StatusCode::UNPROCESSABLE_ENTITY,
    }
}

pub async fn restart_plugin(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> StatusCode {
    if state.manager.get(&id).is_none() {
        return StatusCode::NOT_FOUND;
    }
    match state.manager.restart(&id).await {
        Ok(()) => StatusCode::ACCEPTED,
        Err(_) => StatusCode::UNPROCESSABLE_ENTITY,
    }
}

/// Upper bound on `?lines=` regardless of the plugin's ring-buffer capacity —
/// keeps the query param from being read as an implicit trust of the caller
/// rather than an explicit contract (T-10).
const MAX_LOG_LINES: usize = 10_000;

#[derive(Deserialize)]
pub struct LogsQuery {
    pub lines: Option<usize>,
}

pub async fn get_plugin_logs(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(q): Query<LogsQuery>,
) -> Result<Json<Vec<String>>, StatusCode> {
    if state.manager.get(&id).is_none() && !state.manager.is_supervised(&id) {
        return Err(StatusCode::NOT_FOUND);
    }
    let n = q.lines.unwrap_or(100).min(MAX_LOG_LINES);
    Ok(Json(state.manager.logs(&id, n).await))
}
