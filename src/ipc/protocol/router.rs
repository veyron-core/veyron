use crate::auth::jwt::JwtValidator;
use crate::auth::permissions::{
    check_ipc_send, check_ipc_target, check_permission, normalize_permission,
    required_permission_for_action,
};
use crate::bridge::BridgeHandle;
use crate::events::bus::EventBus;
use crate::events::store::EventStore;
use crate::ipc::connection::Outbound;
use crate::ipc::framing::{Frame, FLAG_RAW_BINARY};
use crate::ipc::messages::IncomingMessage;
use crate::kernel::commands::{CommandHandler, CommandOutcome};
use crate::plugins::registry::{ActionLookup, DeviceMeta, PendingAction, PluginRegistry};
use crate::proto::vynkor::{
    envelope, ActionRequest, ActionRequestChunk, ActionResponse, ActionResponseChunk, ActionStatus,
    DeviceOs, Envelope, ErrorCode, Event, EventPublishAck, EventPublishStatus, KernelCommandAck,
    PermissionType, PluginRegisterAck, Pong, SessionClose,
};
use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use metrics::{counter, histogram};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::helpers::{
    abort_stream, envelope_message_id, is_throttle_exempt, send_envelope, send_error,
    send_register_reject, try_send_envelope, ACTION_CORRELATION_SEQ, EVENT_PUBLISH_SEQ,
};
use crate::ipc::connection::out_frame;
use crate::ipc::framing::target_as_str;

pub struct MessageRouter;

impl MessageRouter {
    pub async fn run(
        rx: mpsc::Receiver<IncomingMessage>,
        registry: Arc<PluginRegistry>,
        event_bus: Arc<EventBus>,
        jwt_validator: Option<Arc<JwtValidator>>,
    ) {
        let defaults = crate::utils::config::Config::default();
        Self::run_with_context(
            rx,
            registry,
            event_bus,
            jwt_validator,
            Instant::now(),
            None,
            None,
            None,
            None,
            None,
            defaults.action_caller_rate_limit_rps,
            defaults.action_caller_max_concurrent,
            defaults.action_timeout_ms,
            defaults.max_conn_errors,
            defaults.max_tracked_error_conns,
            defaults.session_idle_timeout_secs,
            None,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn run_with_context(
        mut rx: mpsc::Receiver<IncomingMessage>,
        registry: Arc<PluginRegistry>,
        event_bus: Arc<EventBus>,
        jwt_validator: Option<Arc<JwtValidator>>,
        start_time: Instant,
        config_path: Option<String>,
        event_store: Option<Arc<EventStore>>,
        mac_secret: Option<Arc<Vec<u8>>>,
        // T-04: operator-declared `config.yaml` `permissions:` allowlist per
        // plugin id. Registration clamps JWT/manifest-claimed permissions to
        // this list so a token can't grant more than the operator configured.
        // `None`/missing-entry plugins (not declared in config.yaml) are left
        // unclamped — matches `validate_plugin_def`'s existing boot-time rule
        // that an absent/empty list means "no restriction".
        config_permissions: Option<Arc<HashMap<String, Vec<String>>>>,
        ipc_rate_limit_rps: Option<u32>,
        // R6-03: per-(caller, provider) action quota. Both None = unlimited,
        // matching ipc_rate_limit_rps's existing opt-in convention.
        action_caller_rate_limit_rps: Option<u32>,
        action_caller_max_concurrent: Option<u32>,
        action_timeout_ms: u32,
        max_conn_errors: u32,
        max_tracked_error_conns: usize,
        // R6-04: idle-timeout bound for accepted streaming sessions. None =
        // disabled, matching action_caller_rate_limit_rps's unlimited convention.
        session_idle_timeout_secs: Option<u32>,
        // E-01: per-device credential store. When auth is on, any registration
        // declaring a device_id must present an active row here, and the
        // frame-MAC key for that connection derives from the row's secret.
        device_store: Option<Arc<crate::auth::device_store::DeviceStore>>,
        // D-06: relay for `role: client` kernels — frames whose target is not
        // in the local registry fall through to the remote host.
        bridge: Option<BridgeHandle>,
    ) {
        // Per-connection protocol-error budget. A connection that produces a burst
        // of malformed/denied/unhandled messages (which each generate an error
        // response) gets throttled: further messages are dropped without a reply,
        // capping the amplification a single misbehaving plugin can cause (VULN-007).
        // A successful message resets the budget, so transient errors don't accrue.
        //
        // Keyed by conn_id -> (count, last_error_at). Pruned by staleness, not
        // registration status (T-08): an unregistered connection is never
        // "registered" so a registration-status prune would keep evicting its
        // own entry back to zero every time the map hit capacity, letting it
        // reset its own budget indefinitely by staying unregistered.
        let mut error_counts: HashMap<u64, (u32, Instant)> = HashMap::new();
        const ERROR_BUDGET_IDLE_TTL: Duration = Duration::from_secs(300);

        // Per-connection IPC send rate limiter keyed by conn_id.
        let ipc_limiter: Option<Arc<DefaultKeyedRateLimiter<u64>>> =
            ipc_rate_limit_rps.and_then(|rps| {
                NonZeroU32::new(rps).map(|r| Arc::new(RateLimiter::keyed(Quota::per_second(r))))
            });

        // R6-03: per-(caller, provider) action rate limiter. Keyed by a tuple so
        // hammering one provider doesn't burn a caller's budget against an
        // unrelated provider it also legitimately calls.
        let action_limiter: Option<Arc<DefaultKeyedRateLimiter<(String, String)>>> =
            action_caller_rate_limit_rps.and_then(|rps| {
                NonZeroU32::new(rps).map(|r| Arc::new(RateLimiter::keyed(Quota::per_second(r))))
            });

        // conn_ids are monotonically assigned and never reused, so without
        // periodic eviction this keyed state grows for the life of the
        // process (AUDIT M-01). Evict idle keys on the same cadence as the
        // error-budget map prune.
        let mut prune_tick = tokio::time::interval(Duration::from_secs(60));
        prune_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            let msg = tokio::select! {
                biased;
                _ = prune_tick.tick() => {
                    if let Some(limiter) = &ipc_limiter {
                        limiter.retain_recent();
                    }
                    if let Some(limiter) = &action_limiter {
                        limiter.retain_recent();
                    }
                    for expired in registry.sweep_expired_actions(Instant::now()) {
                        let response = Envelope {
                            payload: Some(envelope::Payload::ActionResponse(ActionResponse {
                                action_id: expired.original_action_id,
                                status: ActionStatus::ActionTimeout as i32,
                                data_json: vec![],
                                error: "action timed out".to_string(),
                            })),
                            ..Default::default()
                        };
                        send_envelope(&expired.requester_write_tx, response);
                    }
                    if let Some(idle_secs) = session_idle_timeout_secs {
                        let idle_timeout = Duration::from_secs(idle_secs as u64);
                        for (internal_id, _pending) in
                            registry.sweep_idle_sessions(Instant::now(), idle_timeout)
                        {
                            abort_stream(
                                &registry,
                                &internal_id,
                                "idle timeout",
                            )
                            .await;
                        }
                    }
                    continue;
                }
                msg = rx.recv() => match msg {
                    Some(msg) => msg,
                    None => break,
                },
            };
            let conn_id = msg.conn_id;

            // Per-plugin IPC rate limit: send ERR_RATE_LIMITED without disconnecting.
            if let Some(limiter) = &ipc_limiter {
                if limiter.check_key(&conn_id).is_err() {
                    counter!("ipc_send_denied_total").increment(1);
                    send_error(
                        &msg.write_tx,
                        ErrorCode::ErrRateLimited,
                        "IPC rate limit exceeded",
                    );
                    continue;
                }
            }

            let target = match target_as_str(&msg.frame) {
                Some(t) => t.to_string(),
                None => {
                    let raw_hex: String = msg
                        .frame
                        .target
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect();
                    warn!(conn_id, raw_target = %raw_hex, "frame target is not valid UTF-8");
                    send_error(
                        &msg.write_tx,
                        ErrorCode::ErrUnknown,
                        "invalid UTF-8 in frame target",
                    );
                    continue;
                }
            };

            if error_counts.get(&conn_id).map(|(c, _)| *c).unwrap_or(0) >= max_conn_errors {
                if is_throttle_exempt(&msg.frame) {
                    debug!(
                        conn_id,
                        throttled = true,
                        "throttled connection: allowing exempt message through"
                    );
                } else {
                    counter!("ipc_throttled_messages_total").increment(1);
                    continue;
                }
            }

            // D-10: hop-0 trace log. `envelope_message_id` is a best-effort
            // read for observability only — routing stays payload-free
            // (zero-parse); a raw/undecodable payload just logs an empty id.
            let trace_mid = envelope_message_id(&msg.frame);
            let trace_sender = registry
                .get_by_conn_id(msg.conn_id)
                .map(|e| e.plugin_id.clone())
                .unwrap_or_default();
            debug!(
                conn_id,
                message_id = %trace_mid,
                sender_id = %trace_sender,
                target = %target,
                hop = 0,
                "message ingress"
            );

            let errored = match target.as_str() {
                "kernel" => {
                    counter!("messages_routed_total", "routing" => "kernel").increment(1);
                    Self::handle_kernel_message(
                        msg,
                        &registry,
                        &event_bus,
                        &jwt_validator,
                        start_time,
                        config_path.as_deref(),
                        event_store.as_ref(),
                        &mac_secret,
                        config_permissions.as_deref(),
                        action_limiter.as_deref(),
                        action_caller_max_concurrent,
                        action_timeout_ms,
                        device_store.as_deref(),
                    )
                    .await
                }
                "*" => {
                    counter!("messages_routed_total", "routing" => "broadcast").increment(1);
                    Self::broadcast(msg, &trace_mid, &registry).await
                }
                plugin_id => {
                    counter!("messages_routed_total", "routing" => "forward").increment(1);
                    Self::forward(msg, plugin_id, &trace_mid, &registry, bridge.as_ref()).await
                }
            };

            if errored {
                let now = Instant::now();
                if error_counts.len() >= max_tracked_error_conns {
                    error_counts
                        .retain(|_, (_, last)| now.duration_since(*last) < ERROR_BUDGET_IDLE_TTL);
                }
                let entry = error_counts.entry(conn_id).or_insert((0, now));
                entry.0 += 1;
                entry.1 = now;
                if entry.0 == max_conn_errors {
                    warn!(
                        conn_id,
                        threshold = max_conn_errors,
                        "connection exceeded protocol-error budget; throttling further messages"
                    );
                    counter!("ipc_throttled_connections_total").increment(1);
                }
            } else {
                // Well-behaved message — clear any accrued error budget.
                error_counts.remove(&conn_id);
            }
        }
    }

    /// Commands any registered plugin may call without
    /// PERMISSION_KERNEL_ADMIN: read-only discovery only. Manifest data is
    /// public distribution metadata (it ships in registry.json), and none of
    /// these mutate state — everything else (reload_config, list_devices)
    /// stays admin-gated. This is what lets the `agent` plugin read plugin
    /// manifests for tool discovery without holding admin.
    const READONLY_COMMANDS: [&str; 3] = ["health_check", "list_plugins", "get_manifest"];

    // 12 params mirror the kernel's wired components; collapsing them into a
    // config struct is MA-06's scope, not this lint
    #[allow(clippy::too_many_arguments)]
    async fn handle_kernel_message(
        msg: IncomingMessage,
        registry: &PluginRegistry,
        event_bus: &EventBus,
        jwt_validator: &Option<Arc<JwtValidator>>,
        start_time: Instant,
        config_path: Option<&str>,
        event_store: Option<&Arc<EventStore>>,
        mac_secret: &Option<Arc<Vec<u8>>>,
        config_permissions: Option<&HashMap<String, Vec<String>>>,
        action_limiter: Option<&DefaultKeyedRateLimiter<(String, String)>>,
        action_caller_max_concurrent: Option<u32>,
        action_timeout_ms: u32,
        device_store: Option<&crate::auth::device_store::DeviceStore>,
    ) -> bool {
        let envelope = match Envelope::decode(msg.frame.payload.as_ref()) {
            Ok(e) => e,
            Err(_) => {
                send_error(
                    &msg.write_tx,
                    ErrorCode::ErrDeserialization,
                    "decode failed",
                );
                return true;
            }
        };

        // Allow PluginRegister from unregistered senders; all others require registration
        let is_register = matches!(envelope.payload, Some(envelope::Payload::PluginRegister(_)));
        if !is_register && !registry.is_registered(msg.conn_id) {
            send_error(&msg.write_tx, ErrorCode::ErrNotRegistered, "not registered");
            return true;
        }

        match envelope.payload {
            Some(envelope::Payload::PluginRegister(reg)) => {
                let plugin_id = reg.plugin_id.clone();
                let mut manifest = reg.manifest.unwrap_or_default();

                // D-03: reject on protocol_version *major* mismatch (minor/
                // patch accepted). Empty protocol_version = a v1.5 host
                // plugin (or a stale SDK) — accept, it predates the field.
                let wire_major = vynkor_wire::PROTOCOL_VERSION
                    .split('.')
                    .next()
                    .unwrap_or("");
                let plugin_major = reg.protocol_version.split('.').next().unwrap_or(wire_major);
                if !reg.protocol_version.is_empty() && plugin_major != wire_major {
                    send_error(
                        &msg.write_tx,
                        ErrorCode::ErrProtocolMismatch,
                        &format!(
                            "protocol version {} incompatible with kernel {}",
                            reg.protocol_version,
                            vynkor_wire::PROTOCOL_VERSION
                        ),
                    );
                    return true;
                }

                // JWT validation (only when kernel has jwt_secret configured)
                if let Some(validator) = jwt_validator {
                    match validator.validate(&reg.jwt_token) {
                        Ok(claims) => {
                            // D-03: a device-scoped token (sub == device_id)
                            // authorizes every plugin of that device; a
                            // plugin-scoped token (sub == plugin_id) as before.
                            let device_match =
                                !reg.device_id.is_empty() && claims.sub == reg.device_id;
                            if claims.sub != plugin_id && !device_match {
                                send_register_reject(&msg.write_tx, "token plugin_id mismatch");
                                return true;
                            }
                            // Token fields take precedence over manifest declaration
                            manifest.permissions = claims.permissions;
                            manifest.ipc_targets = claims.ipc_targets;
                        }
                        Err(e) => {
                            send_register_reject(&msg.write_tx, &format!("auth failed: {e}"));
                            return true;
                        }
                    }
                }

                // E-01: a device-scoped registration (device_id present) on an
                // auth-enabled kernel must present an active, unexpired
                // credential row; the connection's frame-MAC key then derives
                // from that row's secret instead of the master. Empty device_id
                // = local plugin, unchanged master-secret path.
                let mut device_secret: Option<Vec<u8>> = None;
                if !reg.device_id.is_empty() && mac_secret.is_some() {
                    if let Some(store) = device_store {
                        match store.active_secret(&reg.device_id) {
                            Ok(Some(secret)) => device_secret = Some(secret.into_bytes()),
                            Ok(None) => {
                                send_register_reject(
                                    &msg.write_tx,
                                    &format!(
                                        "unknown device '{}' — pair it via `vyn device connect`",
                                        reg.device_id
                                    ),
                                );
                                return true;
                            }
                            Err(e) => {
                                send_register_reject(&msg.write_tx, &e.to_string());
                                return true;
                            }
                        }
                    }
                }

                // T-04: clamp to the operator's config.yaml allowlist for this
                // plugin id, so a JWT can't grant more than config.yaml allows.
                // No entry for this id (not config.yaml-declared) or an empty
                // list (operator placed no restriction) leaves it unclamped —
                // same convention as `validate_plugin_def`.
                if let Some(allowed) = config_permissions.and_then(|m| m.get(&plugin_id)) {
                    if !allowed.is_empty() {
                        // normalize both sides (N2): config.yaml may list the
                        // lowercase form while the token claims PERMISSION_* names
                        let allowed_norm: HashSet<String> =
                            allowed.iter().map(|a| normalize_permission(a)).collect();
                        let before = manifest.permissions.len();
                        manifest
                            .permissions
                            .retain(|p| allowed_norm.contains(&normalize_permission(p)));
                        if manifest.permissions.len() < before {
                            warn!(
                                plugin_id = %plugin_id,
                                "claimed permissions exceed config.yaml allowlist — clamped"
                            );
                        }
                    }
                }

                let is_mux_device = !reg.capabilities.is_empty()
                    && !reg.device_id.is_empty()
                    && reg.plugin_id == reg.device_id;

                let result = if is_mux_device {
                    registry.register_device(
                        reg.device_id.clone(),
                        msg.conn_id,
                        reg.capabilities.clone(),
                        manifest,
                        msg.write_tx.clone(),
                        DeviceMeta {
                            device_id: reg.device_id.clone(),
                            user_id: reg.user_id.clone(),
                            os: DeviceOs::try_from(reg.os).unwrap_or(DeviceOs::Unspecified),
                            arch: reg.arch.clone(),
                            os_version: reg.os_version.clone(),
                            capabilities: reg.capabilities.clone(),
                        },
                    )
                } else {
                    if !reg.device_id.is_empty()
                        && reg.plugin_id.contains('.')
                        && reg.plugin_id.starts_with(&format!("{}.", reg.device_id))
                    {
                        warn!(
                            plugin_id = %plugin_id,
                            device_id = %reg.device_id,
                            "deprecated per-cap registration: use single-WS with plugin_id=device_id and capabilities=[...]"
                        );
                    }
                    registry.register_with_device(
                        plugin_id.clone(),
                        msg.conn_id,
                        manifest,
                        msg.write_tx.clone(),
                        DeviceMeta {
                            device_id: reg.device_id.clone(),
                            user_id: reg.user_id.clone(),
                            os: DeviceOs::try_from(reg.os).unwrap_or(DeviceOs::Unspecified),
                            arch: reg.arch.clone(),
                            os_version: reg.os_version.clone(),
                            capabilities: reg.capabilities.clone(),
                        },
                    )
                };

                // When auth is on, mint a per-registration nonce; the plugin and
                // kernel both derive the frame-MAC key from it.
                let session_nonce: Vec<u8> = if mac_secret.is_some() && result.is_ok() {
                    use rand::RngCore;
                    let mut n = vec![0u8; crate::auth::frame_mac::SESSION_NONCE_LEN];
                    rand::thread_rng().fill_bytes(&mut n);
                    n
                } else {
                    Vec::new()
                };

                let ack = match &result {
                    Ok(()) => {
                        let granted = registry
                            .get(&plugin_id)
                            .map(|e| e.manifest.permissions.clone())
                            .unwrap_or_default();
                        info!(plugin_id = %plugin_id, "plugin registered");
                        counter!("plugins_registered_total").increment(1);
                        PluginRegisterAck {
                            accepted: true,
                            reject_reason: String::new(),
                            granted_permissions: granted,
                            session_nonce: session_nonce.clone(),
                        }
                    }
                    Err(e) => {
                        warn!(plugin_id = %plugin_id, reason = %e, "registration rejected");
                        PluginRegisterAck {
                            accepted: false,
                            reject_reason: e.to_string(),
                            granted_permissions: vec![],
                            session_nonce: Vec::new(),
                        }
                    }
                };

                let response = Envelope {
                    message_id: envelope.message_id.clone(),
                    payload: Some(envelope::Payload::PluginRegisterAck(ack)),
                    ..Default::default()
                };
                send_envelope(&msg.write_tx, response);

                // Enable the frame MAC for this connection: derive the key, store
                // it for inbound verification, and tell the write loop (ordered
                // after the ack just sent) to start tagging outbound frames.
                if let (Some(secret), true) = (&mac_secret, result.is_ok()) {
                    // E-01: device-scoped connections key the MAC off their own
                    // credential; everything else keeps the master secret.
                    let ikm: &[u8] = match &device_secret {
                        Some(s) => s.as_slice(),
                        None => secret.as_slice(),
                    };
                    let key =
                        crate::auth::frame_mac::derive_session_key(ikm, &session_nonce, &plugin_id);
                    // EnableMac installs the inbound key AND activates outbound tagging
                    // inside the write_loop, after the ack has been written to the socket.
                    // This prevents inbound MAC verification from activating before the
                    // plugin has received the ack (VULN-020).
                    let _ = msg
                        .write_tx
                        .send(Outbound::EnableMac(key, msg.session_key.clone()))
                        .await;
                }

                if result.is_ok() {
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis();
                    event_bus
                        .publish(
                            Event {
                                event_id: format!("sys-joined-{plugin_id}-{now_ms}"),
                                event_type: "system.plugin_joined".to_string(),
                                payload_json: crate::events::bus::plugin_lifecycle_payload(
                                    registry, &plugin_id,
                                ),
                                retry_count: 0,
                            },
                            registry,
                        )
                        .await;
                }
                false
            }

            Some(envelope::Payload::Ping(ping)) => {
                let server_timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let pong = Envelope {
                    message_id: envelope.message_id.clone(),
                    payload: Some(envelope::Payload::Pong(Pong {
                        original_timestamp: ping.timestamp,
                        server_timestamp,
                    })),
                    ..Default::default()
                };
                send_envelope(&msg.write_tx, pong);
                false
            }

            Some(envelope::Payload::Pong(_)) => {
                // watchdog ping response — record the pong
                if let Some(entry) = registry.get_by_conn_id(msg.conn_id) {
                    registry.record_pong(&entry.plugin_id);
                }
                false
            }

            Some(envelope::Payload::Subscribe(sub)) => {
                if let Some(entry) = registry.get_by_conn_id(msg.conn_id) {
                    event_bus.subscribe(&entry.plugin_id, sub.event_types);
                }
                false
            }

            Some(envelope::Payload::Unsubscribe(unsub)) => {
                if let Some(entry) = registry.get_by_conn_id(msg.conn_id) {
                    event_bus.unsubscribe(&entry.plugin_id, unsub.event_types);
                }
                false
            }

            Some(envelope::Payload::EventPublish(req)) => {
                let sender_id = registry
                    .get_by_conn_id(msg.conn_id)
                    .map(|e| e.plugin_id.clone())
                    .unwrap_or_default();

                let (status, event_id) = if check_permission(
                    registry,
                    &sender_id,
                    PermissionType::PermissionEventPublish,
                )
                .is_err()
                {
                    (
                        EventPublishStatus::EventPublishPermissionDeny,
                        String::new(),
                    )
                } else {
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis();
                    let seq = EVENT_PUBLISH_SEQ.fetch_add(1, Ordering::Relaxed);
                    let event_id = format!("evt-{sender_id}-{now_ms}-{seq}");
                    let namespaced_type = format!("plugin.{sender_id}.{}", req.event_type);
                    event_bus
                        .publish(
                            Event {
                                event_id: event_id.clone(),
                                event_type: namespaced_type,
                                payload_json: req.payload_json,
                                retry_count: 0,
                            },
                            registry,
                        )
                        .await;
                    (EventPublishStatus::EventPublishOk, event_id)
                };

                let ack = Envelope {
                    message_id: envelope.message_id.clone(),
                    payload: Some(envelope::Payload::EventPublishAck(EventPublishAck {
                        event_id,
                        status: status as i32,
                        error: String::new(),
                    })),
                    ..Default::default()
                };
                send_envelope(&msg.write_tx, ack);
                false
            }

            Some(envelope::Payload::ActionRequest(req)) => {
                let action_start = Instant::now();
                let action_id = req.action_id.clone();
                let sender_id = registry
                    .get_by_conn_id(msg.conn_id)
                    .map(|e| e.plugin_id.clone())
                    .unwrap_or_default();

                // R5-07 (option b): route to a plugin that declared this action in
                // its manifest — "declared it" is the entire authorization model
                // for actions with no entry in `required_permission_for_action`.
                // Ambiguous declarations (>1 provider) are refused rather than
                // arbitrarily resolved.
                //
                // T-19: for actions that *do* have a required permission, that
                // permission is checked on the requester as well as the provider.
                // Checking the provider alone lets any plugin launder a
                // privileged action through a permitted provider (e.g. an
                // unprivileged plugin calling `http_request` on the `network`
                // provider gets a real network request performed on its
                // behalf) — the provider's grant is authorization for the
                // provider to *perform* the action, not for arbitrary callers
                // to *invoke* it. Actions with no required permission are
                // unaffected: the provider-declares-authorization model still
                // applies to them as-is.
                let not_found_status = match registry.find_action_provider(&req.action) {
                    ActionLookup::NotFound => Some(ActionStatus::ActionNotFound),
                    ActionLookup::Ambiguous(providers) => {
                        warn!(
                            action = %req.action,
                            providers = ?providers,
                            "ambiguous action declaration: multiple providers, refusing to route"
                        );
                        Some(ActionStatus::ActionNotFound)
                    }
                    ActionLookup::Found(provider)
                        if registry
                            .action_requirement(&provider.plugin_id, &req.action)
                            .or_else(|| required_permission_for_action(&req.action))
                            .is_some_and(|perm| {
                                check_permission(registry, &provider.plugin_id, perm).is_err()
                                    || check_permission(registry, &sender_id, perm).is_err()
                            }) =>
                    {
                        Some(ActionStatus::ActionPermissionDeny)
                    }
                    // R6-03: concurrency cap — checked before the rate limit since it's
                    // the direct fix for "one caller holds N provider slots open" and is
                    // cheaper (a DashMap scan, no token-bucket state touch) to fail fast on.
                    ActionLookup::Found(ref provider)
                        if action_caller_max_concurrent.is_some_and(|cap| {
                            registry.count_pending_actions_for(&sender_id, &provider.plugin_id)
                                >= cap
                        }) =>
                    {
                        counter!("action_quota_denied_total", "reason" => "concurrency")
                            .increment(1);
                        Some(ActionStatus::ActionQuotaExceeded)
                    }
                    // R6-03: rate limit — keyed by (caller, provider), same governor
                    // crate/pattern as the existing per-conn ipc_limiter.
                    ActionLookup::Found(ref provider)
                        if action_limiter.is_some_and(|limiter| {
                            limiter
                                .check_key(&(sender_id.clone(), provider.plugin_id.clone()))
                                .is_err()
                        }) =>
                    {
                        counter!("action_quota_denied_total", "reason" => "rate").increment(1);
                        Some(ActionStatus::ActionQuotaExceeded)
                    }
                    ActionLookup::Found(provider) => {
                        let internal_id = format!(
                            "kact-{}",
                            ACTION_CORRELATION_SEQ.fetch_add(1, Ordering::Relaxed)
                        );
                        let effective_timeout_ms = if req.timeout_ms == 0 {
                            action_timeout_ms
                        } else {
                            req.timeout_ms
                        };
                        registry.register_pending_action(
                            internal_id.clone(),
                            PendingAction {
                                requester_write_tx: msg.write_tx.clone(),
                                original_action_id: action_id.clone(),
                                requester_id: sender_id.clone(),
                                deadline: Instant::now()
                                    + Duration::from_millis(effective_timeout_ms as u64),
                                provider_id: provider.plugin_id.clone(),
                                streaming: req.streaming,
                                session_accepted: false,
                                last_activity: Instant::now(),
                            },
                        );

                        let forwarded = Envelope {
                            message_id: envelope.message_id.clone(),
                            payload: Some(envelope::Payload::ActionRequest(ActionRequest {
                                action_id: internal_id,
                                action: req.action.clone(),
                                params_json: req.params_json.clone(),
                                timeout_ms: req.timeout_ms,
                                streaming: req.streaming,
                                // Stamped from the authenticated sender_id, never
                                // from req.caller_plugin_id — the inbound value
                                // (if any) is discarded here, making this field
                                // unspoofable by the caller.
                                caller_plugin_id: sender_id.clone(),
                            })),
                            ..Default::default()
                        };
                        send_envelope(&provider.write_tx, forwarded);
                        None
                    }
                };

                if let Some(status) = not_found_status {
                    let response = Envelope {
                        message_id: envelope.message_id.clone(),
                        payload: Some(envelope::Payload::ActionResponse(ActionResponse {
                            action_id,
                            status: status as i32,
                            data_json: vec![],
                            error: format!("{:?}", status),
                        })),
                        ..Default::default()
                    };
                    send_envelope(&msg.write_tx, response);
                }
                histogram!("action_request_duration_ms")
                    .record(action_start.elapsed().as_millis() as f64);
                false
            }

            Some(envelope::Payload::ActionResponse(resp)) => {
                // A provider plugin answering a kernel-routed ActionRequest always
                // targets "kernel" (it doesn't know who really asked) — this is
                // where the kernel translates the internal correlation id back to
                // the original requester's action_id and proxies the response.
                // Resolve the sender's identity BEFORE touching the pending-action
                // map. We must not remove the entry unless the sender is actually
                // the provider it was routed to — otherwise any registered plugin
                // could spoof or steal another provider's response by guessing the
                // sequential internal action_id (AUDIT: response-spoofing gap).
                let sender_plugin_id = registry
                    .get_by_conn_id(msg.conn_id)
                    .map(|e| e.plugin_id.clone());

                let status_ok = resp.status == ActionStatus::ActionOk as i32;
                let taken = sender_plugin_id.as_ref().and_then(|plugin_id| {
                    registry.resolve_action_response(&resp.action_id, plugin_id, status_ok)
                });

                match taken {
                    Some(pending) => {
                        let response = Envelope {
                            message_id: envelope.message_id.clone(),
                            payload: Some(envelope::Payload::ActionResponse(ActionResponse {
                                action_id: pending.original_action_id,
                                status: resp.status,
                                data_json: resp.data_json,
                                error: resp.error,
                            })),
                            ..Default::default()
                        };
                        send_envelope(&pending.requester_write_tx, response);
                    }
                    None => {
                        warn!(
                            action_id = %resp.action_id,
                            sender = ?sender_plugin_id,
                            "action response with no matching pending request for this sender \
                             (late, duplicate, already timed out, or sender is not the routed \
                             provider), dropping"
                        );
                    }
                }
                false
            }

            Some(envelope::Payload::ActionRequestChunk(chunk)) => {
                let sender_id = registry
                    .get_by_conn_id(msg.conn_id)
                    .map(|e| e.plugin_id.clone())
                    .unwrap_or_default();

                match registry
                    .find_pending_internal_id(&sender_id, &chunk.action_id)
                    .and_then(|internal_id| {
                        registry
                            .get_pending_action(&internal_id)
                            .map(|pending| (internal_id, pending))
                    }) {
                    Some((internal_id, pending)) => {
                        registry.touch_pending_action(&internal_id);
                        match registry.get(&pending.provider_id) {
                            Some(provider_entry) => {
                                let forwarded = Envelope {
                                    message_id: envelope.message_id.clone(),
                                    payload: Some(envelope::Payload::ActionRequestChunk(
                                        ActionRequestChunk {
                                            action_id: internal_id.clone(),
                                            seq: chunk.seq,
                                            chunk: chunk.chunk,
                                            r#final: chunk.r#final,
                                        },
                                    )),
                                    ..Default::default()
                                };
                                if !try_send_envelope(&provider_entry.write_tx, forwarded) {
                                    warn!(action_id = %internal_id, "request chunk forward failed, aborting stream");
                                    abort_stream(registry, &internal_id, "receiver backpressure")
                                        .await;
                                }
                            }
                            None => {
                                warn!(action_id = %internal_id, "request chunk provider disconnected, aborting stream");
                                abort_stream(registry, &internal_id, "provider disconnected").await;
                            }
                        }
                    }
                    None => {
                        warn!(
                            action_id = %chunk.action_id,
                            sender = %sender_id,
                            "request chunk with no matching pending action, dropping"
                        );
                    }
                }
                false
            }

            Some(envelope::Payload::ActionResponseChunk(chunk)) => {
                // Mirrors the ActionResponse arm above: the provider always
                // deals in internal-id space, so chunk.action_id here IS the
                // internal id already — no reverse lookup needed, but the
                // sender must be verified as the actual routed provider
                // before we trust it (same spoofing concern as
                // take_pending_action_if_provider).
                let sender_plugin_id = registry
                    .get_by_conn_id(msg.conn_id)
                    .map(|e| e.plugin_id.clone());

                match sender_plugin_id.and_then(|pid| {
                    registry
                        .get_pending_action(&chunk.action_id)
                        .filter(|pending| pending.provider_id == pid)
                }) {
                    Some(pending) => {
                        registry.touch_pending_action(&chunk.action_id);
                        let forwarded = Envelope {
                            message_id: envelope.message_id.clone(),
                            payload: Some(envelope::Payload::ActionResponseChunk(
                                ActionResponseChunk {
                                    action_id: pending.original_action_id,
                                    seq: chunk.seq,
                                    chunk: chunk.chunk,
                                },
                            )),
                            ..Default::default()
                        };
                        if !try_send_envelope(&pending.requester_write_tx, forwarded) {
                            warn!(action_id = %chunk.action_id, "response chunk forward failed, aborting stream");
                            abort_stream(registry, &chunk.action_id, "receiver backpressure").await;
                        }
                    }
                    None => {
                        warn!(
                            action_id = %chunk.action_id,
                            "response chunk with no matching pending action for this sender, dropping"
                        );
                    }
                }
                false
            }

            Some(envelope::Payload::SessionClose(close)) => {
                let sender_id = match registry.get_by_conn_id(msg.conn_id) {
                    Some(entry) => entry.plugin_id.clone(),
                    None => {
                        send_error(&msg.write_tx, ErrorCode::ErrNotRegistered, "not registered");
                        return true;
                    }
                };

                // SessionClose can come from either peer. The provider always
                // addresses by internal id (mirrors ActionResponseChunk); the
                // requester only knows its own action_id and needs the same
                // reverse lookup ActionRequestChunk uses.
                let resolved = match registry.get_pending_action(&close.action_id) {
                    Some(pending) if pending.provider_id == sender_id => {
                        Some((close.action_id.clone(), pending, true))
                    }
                    _ => registry
                        .find_pending_internal_id(&sender_id, &close.action_id)
                        .and_then(|internal_id| {
                            registry
                                .get_pending_action(&internal_id)
                                .map(|pending| (internal_id, pending, false))
                        }),
                };

                match resolved {
                    Some((internal_id, pending, _)) if !pending.session_accepted => {
                        warn!(
                            action_id = %internal_id,
                            sender = %sender_id,
                            "SessionClose before session acceptance, rejecting"
                        );
                        send_error(
                            &msg.write_tx,
                            ErrorCode::ErrUnknown,
                            "session not accepted, nothing to close",
                        );
                        true
                    }
                    Some((internal_id, pending, from_provider)) => {
                        if from_provider {
                            let forwarded = Envelope {
                                message_id: envelope.message_id.clone(),
                                payload: Some(envelope::Payload::SessionClose(SessionClose {
                                    action_id: pending.original_action_id.clone(),
                                    reason: close.reason.clone(),
                                })),
                                ..Default::default()
                            };
                            let _ = try_send_envelope(&pending.requester_write_tx, forwarded);
                        } else if let Some(provider_entry) = registry.get(&pending.provider_id) {
                            let forwarded = Envelope {
                                message_id: envelope.message_id.clone(),
                                payload: Some(envelope::Payload::SessionClose(SessionClose {
                                    action_id: internal_id.clone(),
                                    reason: close.reason.clone(),
                                })),
                                ..Default::default()
                            };
                            let _ = try_send_envelope(&provider_entry.write_tx, forwarded);
                        }
                        registry.take_pending_action(&internal_id);
                        false
                    }
                    None => {
                        warn!(
                            action_id = %close.action_id,
                            sender = %sender_id,
                            "SessionClose with no matching accepted session, dropping"
                        );
                        send_error(&msg.write_tx, ErrorCode::ErrUnknown, "no matching session");
                        true
                    }
                }
            }

            Some(envelope::Payload::KernelCommand(cmd)) => {
                let sender_id = registry
                    .get_by_conn_id(msg.conn_id)
                    .map(|e| e.plugin_id.clone())
                    .unwrap_or_default();

                let outcome = if !Self::READONLY_COMMANDS.contains(&cmd.command.as_str())
                    && check_permission(registry, &sender_id, PermissionType::PermissionKernelAdmin)
                        .is_err()
                {
                    warn!(
                        sender = %sender_id,
                        command = %cmd.command,
                        "kernel command permission denied"
                    );
                    CommandOutcome::permission_denied(format!(
                        "{sender_id} lacks PERMISSION_KERNEL_ADMIN"
                    ))
                } else {
                    CommandHandler::dispatch(
                        &cmd.command,
                        registry,
                        start_time,
                        config_path,
                        &cmd.params_json,
                    )
                };

                let ack = Envelope {
                    message_id: envelope.message_id.clone(),
                    payload: Some(envelope::Payload::KernelCommandAck(KernelCommandAck {
                        command_id: cmd.command_id,
                        status: outcome.status as i32,
                        data_json: outcome.data_json,
                        error: outcome.error,
                    })),
                    ..Default::default()
                };
                send_envelope(&msg.write_tx, ack);
                false
            }

            Some(envelope::Payload::EventAck(ack)) => {
                if let Some(store) = event_store {
                    store.mark_delivered_async(ack.event_id).await;
                }
                false
            }

            _ => {
                send_error(&msg.write_tx, ErrorCode::ErrUnknown, "unhandled message");
                true
            }
        }
    }

    async fn forward(
        msg: IncomingMessage,
        plugin_id: &str,
        message_id: &str,
        registry: &PluginRegistry,
        bridge: Option<&BridgeHandle>,
    ) -> bool {
        let sender_id = match registry.get_by_conn_id(msg.conn_id) {
            Some(entry) => entry.plugin_id.clone(),
            None => {
                send_error(&msg.write_tx, ErrorCode::ErrNotRegistered, "not registered");
                return true;
            }
        };

        // Default-deny peer-to-peer IPC: sender must hold PERMISSION_IPC_SEND.
        if check_ipc_send(registry, &sender_id).is_err() {
            warn!(sender = %sender_id, target = %plugin_id, "ipc send denied");
            counter!("ipc_send_denied_total").increment(1);
            send_error(
                &msg.write_tx,
                ErrorCode::ErrPermissionDenied,
                "PERMISSION_IPC_SEND required",
            );
            return true;
        }

        {
            let sender_entry = match registry.get(&sender_id) {
                Some(e) => e,
                None => {
                    warn!(sender = %sender_id, "sender vanished between checks");
                    counter!("ipc_send_denied_total").increment(1);
                    send_error(
                        &msg.write_tx,
                        ErrorCode::ErrPermissionDenied,
                        "sender not found",
                    );
                    return true;
                }
            };
            let resolved_for_auth = registry.get_mux(plugin_id);
            if let Some(ref target_entry) = resolved_for_auth {
                if target_entry.user_id != sender_entry.user_id {
                    warn!(
                        sender = %sender_id,
                        target = %plugin_id,
                        sender_user = %sender_entry.user_id,
                        target_user = %target_entry.user_id,
                        "cross-user IPC denied (mux)"
                    );
                    counter!("ipc_send_denied_total").increment(1);
                    send_error(
                        &msg.write_tx,
                        ErrorCode::ErrPermissionDenied,
                        "cross-user IPC denied",
                    );
                    return true;
                }
            }
            let allowed = sender_entry
                .manifest
                .ipc_targets
                .iter()
                .any(|t| t == plugin_id)
                || (plugin_id.contains('.') && resolved_for_auth.is_some() && {
                    if let Some((dev, _)) = plugin_id.split_once('.') {
                        sender_entry.manifest.ipc_targets.iter().any(|t| t == dev)
                    } else {
                        false
                    }
                });
            if !allowed {
                warn!(sender = %sender_id, target = %plugin_id, "ipc target not in allowlist");
                counter!("ipc_send_denied_total").increment(1);
                send_error(
                    &msg.write_tx,
                    ErrorCode::ErrPermissionDenied,
                    "target not in ipc_targets allowlist",
                );
                return true;
            }
        }

        // Audio stream gate (T-06): raw binary frames require PERMISSION_AUDIO_STREAM.
        if msg.frame.flags & FLAG_RAW_BINARY != 0
            && check_permission(registry, &sender_id, PermissionType::PermissionAudioStream)
                .is_err()
        {
            warn!(sender = %sender_id, target = %plugin_id, "audio stream permission denied");
            counter!("ipc_send_denied_total").increment(1);
            send_error(
                &msg.write_tx,
                ErrorCode::ErrPermissionDenied,
                "PERMISSION_AUDIO_STREAM required for FLAG_RAW_BINARY frames",
            );
            return true;
        }

        let resolved = registry.get_mux(plugin_id);
        let is_mux = resolved.is_some() && registry.get(plugin_id).is_none();
        if is_mux {
            counter!("ipc_forward_mux_total").increment(1);
        }
        match resolved {
            Some(entry) => {
                // For device caps via direct target with empty action (mux single-WS),
                // create a pending so the ActionResponse can be routed back to the caller.
                // This mirrors the kernel-routed ActionRequest pending path.
                if let Ok(env) = Envelope::decode(msg.frame.payload.as_ref()) {
                    if let Some(envelope::Payload::ActionRequest(req)) = env.payload {
                        if req.action.is_empty() && plugin_id.contains('.') {
                            let pending = PendingAction {
                                requester_write_tx: msg.write_tx.clone(),
                                original_action_id: req.action_id.clone(),
                                requester_id: sender_id.clone(),
                                deadline: Instant::now() + Duration::from_millis(30000),
                                provider_id: entry.plugin_id.clone(),
                                streaming: req.streaming,
                                session_accepted: false,
                                last_activity: Instant::now(),
                            };
                            registry.register_pending_action(req.action_id.clone(), pending);
                        }
                    }
                }
                // Strip FLAG_MAC_PRESENT: the recipient's write_loop re-tags with its own
                // session key. Forwarding the sender's flag without a fresh tag corrupts
                // the stream (mirrors broadcast()).
                let frame = Frame {
                    magic: msg.frame.magic,
                    flags: msg.frame.flags & !crate::ipc::framing::FLAG_MAC_PRESENT,
                    length: msg.frame.length,
                    target: msg.frame.target,
                    crc32: msg.frame.crc32,
                    payload: msg.frame.payload.clone(),
                    mac: None,
                };
                // Non-blocking send: a slow/full target must not block the router.
                // Dropping one frame for a non-draining plugin is not the sender's
                // fault, so this is not counted against the sender's error budget.
                debug!(
                    message_id = %message_id,
                    sender_id = %sender_id,
                    target = %plugin_id,
                    hop = 1,
                    "message relayed to target"
                );
                if entry.write_tx.try_send(out_frame(frame)).is_err() {
                    warn!(target = %plugin_id, "forward: target channel full, frame dropped");
                    counter!("ipc_forward_timeouts_total").increment(1);
                }
                false
            }
            None => {
                // D-06: a `role: client` kernel relays unresolvable targets to
                // the remote host before failing — the frame's target may live
                // on the host (or behind another bridged device).
                if let Some(b) = bridge {
                    if b.relay_to_host(&msg.frame) {
                        return false;
                    }
                }
                warn!(target = %plugin_id, "forward: unknown target");
                send_error(&msg.write_tx, ErrorCode::ErrUnknown, "plugin not found");
                true
            }
        }
    }

    async fn broadcast(msg: IncomingMessage, message_id: &str, registry: &PluginRegistry) -> bool {
        let sender_id = match registry.get_by_conn_id(msg.conn_id) {
            Some(entry) => entry.plugin_id.clone(),
            None => {
                send_error(&msg.write_tx, ErrorCode::ErrNotRegistered, "not registered");
                return true;
            }
        };

        // Default-deny: broadcasting is peer-to-peer fan-out — same gate as unicast.
        if check_ipc_send(registry, &sender_id).is_err() {
            warn!(sender = %sender_id, "broadcast denied");
            counter!("ipc_send_denied_total").increment(1);
            send_error(
                &msg.write_tx,
                ErrorCode::ErrPermissionDenied,
                "PERMISSION_IPC_SEND required",
            );
            return true;
        }

        // Audio stream gate (T-06): raw binary broadcast requires PERMISSION_AUDIO_STREAM.
        if msg.frame.flags & FLAG_RAW_BINARY != 0
            && check_permission(registry, &sender_id, PermissionType::PermissionAudioStream)
                .is_err()
        {
            warn!(sender = %sender_id, "audio stream broadcast denied");
            counter!("ipc_send_denied_total").increment(1);
            send_error(
                &msg.write_tx,
                ErrorCode::ErrPermissionDenied,
                "PERMISSION_AUDIO_STREAM required for FLAG_RAW_BINARY frames",
            );
            return true;
        }

        let entries = registry.list();
        for entry in entries {
            if entry.conn_id == msg.conn_id {
                continue; // skip sender
            }
            // Per-target allowlist check: mirrors forward(). Empty ipc_targets = deny-all.
            if check_ipc_target(registry, &sender_id, &entry.plugin_id).is_err() {
                counter!("ipc_send_denied_total").increment(1);
                continue;
            }
            // Strip FLAG_MAC_PRESENT: the recipient's write_loop re-tags with its own
            // session key. Forwarding the sender's flag without a fresh tag corrupts the stream.
            let frame = Frame {
                magic: msg.frame.magic,
                flags: msg.frame.flags & !crate::ipc::framing::FLAG_MAC_PRESENT,
                length: msg.frame.length,
                target: msg.frame.target,
                crc32: msg.frame.crc32,
                payload: msg.frame.payload.clone(),
                mac: None,
            };
            if entry.write_tx.try_send(out_frame(frame)).is_err() {
                warn!(
                    plugin_id = %entry.plugin_id,
                    "broadcast: target channel full, frame dropped"
                );
                counter!("broadcast_timeouts_total").increment(1);
            } else {
                debug!(
                    message_id = %message_id,
                    sender_id = %sender_id,
                    target = %entry.plugin_id,
                    hop = 1,
                    "broadcast delivered to subscriber"
                );
            }
        }
        false
    }
}
