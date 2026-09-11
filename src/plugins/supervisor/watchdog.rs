use super::*;

use crate::ipc::connection::out_frame;
use crate::ipc::framing::build_frame;
#[cfg(target_os = "linux")]
use crate::plugins::metrics::proc_resource_usage;
use crate::proto::vynkor::{envelope, Envelope, Event, Ping};
use metrics::{counter, gauge};
use prost::Message;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

impl PluginSupervisor {
    pub async fn monitor_loop(self: &Arc<Self>) {
        let mut rx = self.event_rx.lock().await;
        while let Some(event) = rx.recv().await {
            // A manual restart (POST /restart) forces respawn regardless of policy.
            let forced = self.forced_restarts.remove(&event.plugin_id).is_some();

            // B1: an exit is only actionable for the currently registered
            // instance — a stale event from an older spawn must not be
            // attributed to a newer one. B3: an exit after an explicit stop
            // must not restart anything.
            let is_current = self
                .entries
                .get(&event.plugin_id)
                .map(|e| e.epoch == event.epoch)
                .unwrap_or(false);
            let was_stopped = self
                .stopped_epochs
                .get(&event.plugin_id)
                .map(|s| *s == event.epoch)
                .unwrap_or(false);
            if !is_current || was_stopped {
                info!(
                    plugin_id = %event.plugin_id,
                    epoch = event.epoch,
                    is_current,
                    was_stopped,
                    "ignoring stale or stopped exit event"
                );
                continue;
            }

            let decision = self.entries.get(&event.plugin_id).and_then(|entry| {
                let should = forced
                    || match entry.config.restart_policy {
                        RestartPolicy::Never => false,
                        RestartPolicy::Always => entry.restart_count < entry.config.max_restarts,
                        RestartPolicy::OnFailure => {
                            !event.success && entry.restart_count < entry.config.max_restarts
                        }
                    };
                if should {
                    Some((entry.config.clone(), entry.restart_count))
                } else {
                    None
                }
            });

            let will_restart = decision.is_some();
            let restart_count = self
                .entries
                .get(&event.plugin_id)
                .map(|e| e.restart_count)
                .unwrap_or(0);
            info!(
                plugin_id = %event.plugin_id,
                pid = event.pid,
                success = event.success,
                will_restart = will_restart,
                restart_count = restart_count,
                "plugin exited"
            );

            if let (Some(bus), Some(reg)) = (&self.event_bus, &self.plugin_registry) {
                let payload = format!(
                    r#"{{"plugin_id":"{}","restart_count":{},"will_restart":{}}}"#,
                    event.plugin_id, restart_count, will_restart
                );
                bus.publish(
                    Event {
                        event_id: format!("sys-died-{}-{}", event.plugin_id, restart_count),
                        event_type: "system.plugin_died".to_string(),
                        payload_json: payload.into_bytes(),
                        retry_count: 0,
                    },
                    reg,
                )
                .await;
            }

            match decision {
                Some((config, prev_count)) => {
                    let new_count = prev_count + 1;
                    info!(
                        plugin_id = %config.plugin_id,
                        restart_count = new_count,
                        "restarting plugin"
                    );
                    counter!("plugin_restarts_total", "plugin_id" => config.plugin_id.clone())
                        .increment(1);
                    tokio::time::sleep(self.backoff_delay(new_count)).await;
                    // B3: an explicit stop during the backoff window cancels
                    // the restart — stop is terminal.
                    let stopped_during_backoff = self
                        .stopped_epochs
                        .get(&config.plugin_id)
                        .map(|s| *s == event.epoch)
                        .unwrap_or(false);
                    if stopped_during_backoff {
                        info!(
                            plugin_id = %config.plugin_id,
                            epoch = event.epoch,
                            "restart cancelled — plugin stopped during backoff"
                        );
                        continue;
                    }
                    let _ = self
                        .spawn_internal(config, new_count, Some(event.epoch))
                        .await;
                }
                None => {
                    // max restarts reached or Never policy — remove dead entry so
                    // is_running() returns false (VULN-018). Preserve final restart_count
                    // in stopped_counts for historical lookup.
                    let final_count = self
                        .entries
                        .remove(&event.plugin_id)
                        .map(|(_, e)| e.restart_count)
                        .unwrap_or(0);
                    self.stopped_counts.insert(event.plugin_id, final_count);
                }
            }
        }
    }

    pub async fn watchdog_loop(
        self: Arc<Self>,
        registry: Arc<PluginRegistry>,
        interval: Duration,
        timeout: Duration,
    ) {
        let deadline = interval + timeout;
        loop {
            tokio::time::sleep(interval).await;

            let supervised: Vec<(String, u32, Option<u32>)> = self
                .entries
                .iter()
                .map(|e| (e.key().clone(), e.value().pid, e.value().shim_pid))
                .collect();

            // PERF-4 + T-07: per-plugin resource metrics, Linux only. /proc
            // reads are blocking file I/O and this loop is a single shared
            // task — batch the whole sweep into one blocking thread instead
            // of stalling the async worker per pid. Reads the plugin pid,
            // not the shim's.
            #[cfg(target_os = "linux")]
            let samples: Vec<Option<(f64, f64)>> = tokio::task::spawn_blocking({
                let supervised = supervised.clone();
                move || {
                    supervised
                        .into_iter()
                        .map(|(_, pid, _)| proc_resource_usage(pid))
                        .collect()
                }
            })
            .await
            .unwrap_or_default();
            #[cfg(target_os = "linux")]
            for ((plugin_id, _, _), sample) in supervised.iter().zip(samples) {
                if let Some((cpu, rss)) = sample {
                    gauge!("vynkor_plugin_cpu_seconds_total", "plugin_id" => plugin_id.clone())
                        .set(cpu);
                    gauge!("vynkor_plugin_memory_rss_bytes", "plugin_id" => plugin_id.clone())
                        .set(rss);
                }
            }

            for (plugin_id, pid, shim_pid) in supervised {
                if let Some(last_pong) = registry.last_pong(&plugin_id) {
                    if last_pong.elapsed() > deadline {
                        warn!(plugin_id = %plugin_id, "watchdog: plugin unresponsive, sending SIGKILL");
                        let target = shim_pid.unwrap_or(pid) as i32;
                        let _ = nix::sys::signal::kill(
                            nix::unistd::Pid::from_raw(target),
                            nix::sys::signal::Signal::SIGKILL,
                        );
                        // Do NOT reset pong here (VULN-021): the deadline must keep
                        // running so the watchdog can escalate (another SIGKILL) if the
                        // process is stuck in D-state. If the process truly died, the
                        // exit event from monitor_loop will clean up the entry.
                        continue;
                    }
                }

                if let Some(reg_entry) = registry.get(&plugin_id) {
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let env = Envelope {
                        payload: Some(envelope::Payload::Ping(Ping { timestamp })),
                        ..Default::default()
                    };
                    let mut payload = Vec::new();
                    if env.encode(&mut payload).is_ok() {
                        // same shared-task rationale as PERF-1: a plugin that
                        // stops draining its channel must not stall the
                        // watchdog for every other plugin
                        if reg_entry
                            .write_tx
                            .try_send(out_frame(build_frame("client", 0, payload)))
                            .is_err()
                        {
                            counter!("watchdog_pings_dropped_total").increment(1);
                        }
                    }
                }
            }
        }
    }
}
