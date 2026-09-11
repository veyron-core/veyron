use crate::api::display::device_os_str;
use crate::events::store::EventStore;
use crate::ipc::connection::out_frame;
use crate::ipc::framing::build_frame;
use crate::ipc::protocol::kernel_message_id;
use crate::plugins::registry::PluginRegistry;
use crate::proto::vynkor::{envelope, Envelope, Event};
use dashmap::DashMap;
use metrics::counter;
use prost::Message;
use std::collections::HashSet;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{debug, warn};

pub struct EventBus {
    // event_type → set of plugin_ids
    subscriptions: DashMap<String, HashSet<String>>,
    // plugin_id → set of event_types (for fast unsubscribe_all)
    by_plugin: DashMap<String, HashSet<String>>,
    // decided once (orchestrator attaches after EventStore::new succeeds);
    // the caller's Arc stays authoritative — swapping it broke test harnesses
    store: OnceLock<Option<Arc<EventStore>>>,
}

impl EventBus {
    pub fn new() -> Self {
        EventBus {
            subscriptions: DashMap::new(),
            by_plugin: DashMap::new(),
            store: OnceLock::new(),
        }
    }

    pub fn with_store(store: Arc<EventStore>) -> Self {
        EventBus {
            subscriptions: DashMap::new(),
            by_plugin: DashMap::new(),
            store: OnceLock::from(Some(store)),
        }
    }

    /// first attach wins; later calls are no-ops
    pub fn set_store(&self, store: Arc<EventStore>) {
        let _ = self.store.set(Some(store));
    }

    fn event_store(&self) -> Option<Arc<EventStore>> {
        self.store.get().and_then(|s| s.clone())
    }

    pub fn subscribe(&self, plugin_id: &str, event_types: Vec<String>) {
        for event_type in &event_types {
            self.subscriptions
                .entry(event_type.clone())
                .or_default()
                .insert(plugin_id.to_string());
        }
        self.by_plugin
            .entry(plugin_id.to_string())
            .or_default()
            .extend(event_types);
    }

    pub fn unsubscribe(&self, plugin_id: &str, event_types: Vec<String>) {
        for event_type in &event_types {
            if let Some(mut subs) = self.subscriptions.get_mut(event_type) {
                subs.remove(plugin_id);
            }
        }
        if let Some(mut types) = self.by_plugin.get_mut(plugin_id) {
            for t in &event_types {
                types.remove(t);
            }
        }
    }

    pub fn unsubscribe_all(&self, plugin_id: &str) {
        if let Some((_, types)) = self.by_plugin.remove(plugin_id) {
            for event_type in types {
                if let Some(mut subs) = self.subscriptions.get_mut(&event_type) {
                    subs.remove(plugin_id);
                }
            }
        }
    }

    pub fn subscribers(&self, event_type: &str) -> Vec<String> {
        self.subscriptions
            .get(event_type)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub async fn publish(&self, event: Event, registry: &PluginRegistry) {
        if let Some(store) = self.event_store() {
            store.persist_async(event.clone()).await;
        }
        self.deliver(event, registry).await;
    }

    /// Deliver event to subscribers without persisting to store. Used by retry worker.
    pub async fn redeliver(&self, event: Event, registry: &PluginRegistry) {
        self.deliver(event, registry).await;
    }

    async fn deliver(&self, event: Event, registry: &PluginRegistry) {
        let mut targets: HashSet<String> = HashSet::new();

        if let Some(subs) = self.subscriptions.get(&event.event_type) {
            targets.extend(subs.iter().cloned());
        }
        if let Some(wildcards) = self.subscriptions.get("*") {
            targets.extend(wildcards.iter().cloned());
        }

        if targets.is_empty() {
            debug!(event_type = %event.event_type, "event published with no subscribers");
            return;
        }

        let event_type = event.event_type.clone();
        // D-10: the bus builds a fresh envelope per delivery (nothing to
        // preserve from a publisher that never stamped one) — stamp the trace
        // header so each delivered event is a traceable hop with the same
        // kernel-wide id space as build_outbound.
        let env = Envelope {
            message_id: kernel_message_id(),
            sender_id: "kernel".to_string(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            payload: Some(envelope::Payload::Event(event)),
        };
        let mut payload = Vec::new();
        if env.encode(&mut payload).is_err() {
            return;
        }
        // Encoded once per publish; each subscriber below gets an Arc clone
        // (refcount bump) of these bytes, not its own copy.
        let payload: Arc<[u8]> = payload.into();
        let message_id = env.message_id;

        for plugin_id in targets {
            match registry.get(&plugin_id) {
                Some(entry) => {
                    let frame = build_frame(&plugin_id, 0, payload.clone());
                    // Non-blocking send: a slow/full subscriber must not stall the
                    // publisher or any other subscriber in this fan-out loop.
                    match entry.write_tx.try_send(out_frame(frame)) {
                        Ok(()) => {
                            debug!(
                                message_id = %message_id,
                                sender_id = "kernel",
                                target = %plugin_id,
                                hop = 1,
                                event_type = %event_type,
                                "event delivered to subscriber"
                            );
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            // receiver dropped — plugin is disconnecting
                            counter!("events_dropped_total", "reason" => "channel_closed")
                                .increment(1);
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!(
                                plugin_id = %plugin_id,
                                event_type = %event_type,
                                "event dropped: subscriber write channel full"
                            );
                            counter!("events_dropped_total", "reason" => "slow_subscriber")
                                .increment(1);
                        }
                    }
                }
                None => {
                    warn!(
                        plugin_id = %plugin_id,
                        event_type = %event_type,
                        "event dropped: subscriber not in registry"
                    );
                    counter!("events_dropped_total", "reason" => "unregistered").increment(1);
                }
            }
        }
    }
}

pub async fn run_retry_worker(
    store: Arc<EventStore>,
    bus: Arc<EventBus>,
    registry: Arc<PluginRegistry>,
    max_retries: u32,
    retention_secs: u64,
) {
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;
        // PERF-2: the whole SQLite sweep (pending scan + retry bump + prune)
        // runs in one blocking task — none of it touches a tokio worker.
        // Redelivery below stays async; bumping retries before redelivery
        // preserves the original per-event order.
        let (pending, pruned) = tokio::task::spawn_blocking({
            let store = Arc::clone(&store);
            move || {
                let pending = store.pending_older_than(10);
                for event in &pending {
                    store.increment_retry_or_dead(&event.event_id, max_retries);
                }
                (pending, store.prune(retention_secs))
            }
        })
        .await
        .unwrap_or_default();
        for event in pending {
            bus.redeliver(event, &registry).await;
        }
        if pruned > 0 {
            debug!(count = pruned, "EventStore: pruned terminal events");
        }
    }
}

// D-04: payload for system.plugin_joined/plugin_left. serde_json (not format!)
// because device_id/capabilities arrive off the wire unvalidated — a raw
// format! splice would be a JSON-injection vector. Looked up via the registry
// because the caller still holds the entry at publish time.
pub fn plugin_lifecycle_payload(registry: &PluginRegistry, plugin_id: &str) -> Vec<u8> {
    let device_id = registry
        .get(plugin_id)
        .map(|e| e.device_id.clone())
        .unwrap_or_default();
    let (os, capabilities) = match registry.get_device(&device_id) {
        Some(dev) => (device_os_str(dev.os).to_string(), dev.capabilities),
        None => ("unspecified".to_string(), vec![]),
    };
    // D-08: surface the action schema (action_specs) so clients can enumerate
    // callable actions from the joined event alone. Registry data only — the
    // kernel never interprets params_schema.
    let action_specs: Vec<serde_json::Value> = registry
        .get(plugin_id)
        .map(|e| e.manifest.action_specs.clone())
        .unwrap_or_default()
        .iter()
        .map(|s| {
            serde_json::json!({
                "name": s.name,
                "description": s.description,
                "params_schema": s.params_schema,
                "risk": crate::plugins::registry::action_risk_str(s.risk),
                "requires_confirmation": s.requires_confirmation,
            })
        })
        .collect();
    serde_json::json!({
        "plugin_id": plugin_id,
        "device_id": device_id,
        "os": os,
        "capabilities": capabilities,
        "action_specs": action_specs,
    })
    .to_string()
    .into_bytes()
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}
