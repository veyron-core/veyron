use crate::plugins::registry::PendingAction;
use crate::plugins::registry::PluginRegistry;
use crate::proto::vynkor::ActionStatus;
use crate::proto::vynkor::ActionStreamAbort;
use crate::proto::vynkor::{envelope, Envelope};
use metrics::counter;
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::ipc::connection::{out_frame, Outbound};
use crate::ipc::framing::build_frame;
use crate::ipc::framing::Frame;

pub(crate) static MSG_SEQ: AtomicU64 = AtomicU64::new(0);
pub(crate) static ACTION_CORRELATION_SEQ: AtomicU64 = AtomicU64::new(0);
pub(crate) static EVENT_PUBLISH_SEQ: AtomicU64 = AtomicU64::new(0);

/// ma-08: process-wide and never reset in prod — tests that depend on
/// sequence ordering across runs call this in their setup
#[cfg(test)]
pub(crate) fn reset_for_test() {
    MSG_SEQ.store(0, Ordering::Relaxed);
    ACTION_CORRELATION_SEQ.store(0, Ordering::Relaxed);
    EVENT_PUBLISH_SEQ.store(0, Ordering::Relaxed);
}

/// D-10: process-unique trace id for kernel-stamped envelopes. Shared by
/// `build_outbound` and the event bus so the two stamping sites can never
/// collide on the same `k-{ts}-{seq}` value.
pub(crate) fn kernel_message_id() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let seq = MSG_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("k-{ts}-{seq}")
}

/// D-10: best-effort read of the envelope's `message_id` for the trace logs.
/// Observability only — never gates or alters routing (zero-parse preserved);
/// payloads that aren't envelopes (e.g. `FLAG_RAW_BINARY` audio chunks) simply
/// log an empty id.
pub(crate) fn envelope_message_id(frame: &Frame) -> String {
    Envelope::decode(frame.payload.as_ref())
        .map(|env| env.message_id)
        .unwrap_or_default()
}

/// Throttled connections drop further messages without a reply to cap
/// amplification (VULN-007). Provider replies must be exempt or a burst of
/// preceding errors (e.g. many `tts_speak` audio chunk forwards denied by
/// `ipc_targets`) would stall the caller: the final `ActionResponse` would
/// be silently dropped, exactly the supervised-stall observed for sherpa
/// TTS. `Pong` is also exempt or the watchdog would SIGKILL a throttled
/// plugin that is otherwise healthy.
pub(crate) fn is_throttle_exempt(frame: &Frame) -> bool {
    let Ok(env) = Envelope::decode(frame.payload.as_ref()) else {
        return false;
    };
    matches!(
        env.payload,
        Some(envelope::Payload::ActionResponse(_))
            | Some(envelope::Payload::ActionResponseChunk(_))
            | Some(envelope::Payload::ActionRequestChunk(_))
            | Some(envelope::Payload::SessionClose(_))
            | Some(envelope::Payload::Pong(_))
    )
}

/// Builds the outbound wire frame for `env`, or `None` if encoding
/// failed — mirrors the original `send_envelope`'s silent early-return
/// on an encode error (unchanged behavior, just factored out so both
/// send paths share it).
pub(crate) fn build_outbound(mut env: Envelope) -> Option<Outbound> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    // D-10: a caller that already set `message_id` (a trace id preserved
    // from an inbound envelope) keeps it; only stamp a fresh id when
    // there isn't one — otherwise the kernel hop breaks the trace.
    if env.message_id.is_empty() {
        env.message_id = kernel_message_id();
    }
    env.timestamp = ts;
    env.sender_id = "kernel".to_string();
    debug!(
        message_id = %env.message_id,
        sender_id = %env.sender_id,
        target = "client",
        hop = 1,
        "kernel message dispatched"
    );

    let mut payload = Vec::new();
    if env.encode(&mut payload).is_err() {
        return None;
    }
    Some(out_frame(build_frame("client", 0, payload)))
}

/// Kernel→connection envelope send. Non-blocking (PERF-1/T-03): the
/// router task is shared by every connection, so awaiting one peer's
/// full write channel stalls all IPC. A full channel drops the reply
/// (counted); a closed channel means the connection is already gone.
/// Stream forwards that must react to a drop use [`try_send_envelope`].
pub(crate) fn send_envelope(tx: &mpsc::Sender<Outbound>, env: Envelope) {
    if let Some(out) = build_outbound(env) {
        match tx.try_send(out) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!("kernel reply dropped: peer write channel full");
                counter!("kernel_replies_dropped_total").increment(1);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                debug!("kernel reply dropped: peer write channel closed");
            }
        }
    }
}

/// Non-blocking counterpart of `send_envelope` that reports whether the
/// frame was enqueued, for forwarding to a *different* connection's
/// channel (R6-02 stream chunks); callers are responsible for reacting
/// to a drop (R6-02: abort the whole stream — see `abort_stream`).
pub(crate) fn try_send_envelope(tx: &mpsc::Sender<Outbound>, env: Envelope) -> bool {
    match build_outbound(env) {
        Some(out) => tx.try_send(out).is_ok(),
        None => false,
    }
}

pub(crate) fn send_error(
    tx: &mpsc::Sender<Outbound>,
    code: crate::proto::vynkor::ErrorCode,
    message: &str,
) {
    let env = Envelope {
        payload: Some(envelope::Payload::Error(
            crate::proto::vynkor::ErrorMessage {
                code: code as i32,
                message: message.to_string(),
                details: String::new(),
            },
        )),
        ..Default::default()
    };
    send_envelope(tx, env);
}

pub(crate) fn send_register_reject(tx: &mpsc::Sender<Outbound>, reason: &str) {
    let ack = crate::proto::vynkor::PluginRegisterAck {
        accepted: false,
        reject_reason: reason.to_string(),
        granted_permissions: vec![],
        session_nonce: Vec::new(),
    };
    let env = Envelope {
        payload: Some(envelope::Payload::PluginRegisterAck(ack)),
        ..Default::default()
    };
    send_envelope(tx, env);
}

/// R6-02/R6-04: abort an in-flight stream — remove its pending-action
/// slot and notify both sides. Called whenever `try_send_envelope` fails
/// while forwarding a stream chunk in either direction, so a full/closed
/// channel never means a silently dropped (and therefore corrupting)
/// chunk — the whole stream dies instead, loudly, on both ends.
pub(crate) async fn abort_stream(registry: &PluginRegistry, internal_id: &str, reason: &str) {
    let Some(pending) = registry.take_pending_action(internal_id) else {
        return;
    };
    notify_forced_termination(registry, internal_id, pending, reason).await;
}

/// Shared by `abort_stream` (backpressure/disconnect, R6-02) and the
/// idle-timeout sweep (R6-04). Sends `ActionStreamAbort` to both sides.
///
/// R6-04: `pending.session_accepted` decides whether the requester also
/// gets a terminal `ActionResponse{ACTION_STREAM_BACKPRESSURE}`. Before
/// acceptance the requester is still awaiting its *first* (and only
/// expected) `ActionResponse`, so one must be synthesized here — this is
/// unchanged R6-02 behavior. Once a session is accepted, the requester
/// already received the real accepting `ActionResponse{OK}`; sending a
/// second `ActionResponse` for the same `action_id` would be a
/// surprising duplicate the requester never expects, so only
/// `ActionStreamAbort` is sent. The idle-timeout sweep only ever finds
/// accepted sessions (see `sweep_idle_sessions`), so this branch is
/// always taken for that caller — no special-casing needed there.
///
/// Both notification sends are non-blocking (`try_send_envelope`): this
/// is invoked from the shared router loop, which must never block on
/// any single connection's channel (see the non-blocking-in-shared-loop
/// invariant at `forward()`). Best-effort delivery is acceptable: there
/// is no further fallback if even the abort notice can't be sent.
pub(crate) async fn notify_forced_termination(
    registry: &PluginRegistry,
    internal_id: &str,
    pending: PendingAction,
    reason: &str,
) {
    counter!("action_stream_aborted_total", "reason" => reason.to_string()).increment(1);

    let abort_to_requester = Envelope {
        payload: Some(envelope::Payload::ActionStreamAbort(ActionStreamAbort {
            action_id: pending.original_action_id.clone(),
            reason: reason.to_string(),
        })),
        ..Default::default()
    };
    let _ = try_send_envelope(&pending.requester_write_tx, abort_to_requester);

    if !pending.session_accepted {
        let terminal_response = Envelope {
            payload: Some(envelope::Payload::ActionResponse(
                crate::proto::vynkor::ActionResponse {
                    action_id: pending.original_action_id,
                    status: ActionStatus::ActionStreamBackpressure as i32,
                    data_json: vec![],
                    error: reason.to_string(),
                },
            )),
            ..Default::default()
        };
        let _ = try_send_envelope(&pending.requester_write_tx, terminal_response);
    }

    if let Some(provider_entry) = registry.get(&pending.provider_id) {
        let abort_to_provider = Envelope {
            payload: Some(envelope::Payload::ActionStreamAbort(ActionStreamAbort {
                action_id: internal_id.to_string(),
                reason: reason.to_string(),
            })),
            ..Default::default()
        };
        // Best-effort — if the provider's channel is also the one that's
        // full, it'll simply never see this notice and will discover the
        // stream is dead the next time it tries to send a chunk.
        let _ = try_send_envelope(&provider_entry.write_tx, abort_to_provider);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_for_test_zeroes_all_sequence_atomics() {
        // drive all three past zero first
        let _ = kernel_message_id();
        let _ = ACTION_CORRELATION_SEQ.fetch_add(1, Ordering::Relaxed);
        let _ = EVENT_PUBLISH_SEQ.fetch_add(1, Ordering::Relaxed);
        assert_ne!(MSG_SEQ.load(Ordering::Relaxed), 0);

        reset_for_test();

        // tight window: a concurrent lib-test bump between store and load is
        // theoretically possible but the counters only move per envelope
        assert_eq!(MSG_SEQ.load(Ordering::Relaxed), 0);
        assert_eq!(ACTION_CORRELATION_SEQ.load(Ordering::Relaxed), 0);
        assert_eq!(EVENT_PUBLISH_SEQ.load(Ordering::Relaxed), 0);
    }
}
