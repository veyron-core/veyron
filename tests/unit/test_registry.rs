use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use vynkor::plugins::registry::{
    plugin_state_str, DeviceMeta, DeviceOs, DeviceState, PluginRegistry, PluginState,
};
use vynkor::proto::vynkor::PluginManifest;
use vynkor::utils::errors::VynkorError;

fn dummy_write_tx() -> mpsc::Sender<vynkor::ipc::connection::Outbound> {
    mpsc::channel::<vynkor::ipc::connection::Outbound>(1).0
}

fn dummy_manifest() -> PluginManifest {
    PluginManifest {
        permissions: vec![],
        actions: vec![],
        events: vec![],
        ipc_targets: vec![],
        ..Default::default()
    }
}

#[test]
fn register_then_get_returns_entry() {
    let reg = PluginRegistry::new();

    reg.register(
        "weather".to_string(),
        42,
        dummy_manifest(),
        dummy_write_tx(),
        "",
        "",
    )
    .expect("register must succeed");

    let entry = reg.get("weather").expect("get must return entry");
    assert_eq!(entry.plugin_id, "weather");
    assert_eq!(entry.conn_id, 42);
    assert!(matches!(entry.state, PluginState::Registered));
    // D-02: a plugin that declares no device identity is a host plugin
    assert_eq!(entry.device_id, "local");
    assert_eq!(entry.user_id, "default");
}

#[test]
fn plugin_state_str_is_stable_lowercase() {
    // UX-2: the public string must not be a Rust Debug enum name
    assert_eq!(plugin_state_str(&PluginState::Registered), "registered");
}

#[test]
fn register_rejects_invalid_plugin_ids() {
    let reg = PluginRegistry::new();

    let long = "x".repeat(33);
    let bad: Vec<&str> = vec![
        "",                            // empty
        "kernel",                      // reserved routing target
        "*",                           // reserved broadcast target
        r#"evil","admin":true,"x":""#, // JSON-injection attempt
        "has space",                   // disallowed char
        "tab\tthing",                  // control char
        &long,                         // exceeds 32-byte target field
    ];

    for id in bad {
        let res = reg.register(
            id.to_string(),
            1,
            dummy_manifest(),
            dummy_write_tx(),
            "",
            "",
        );
        assert!(
            matches!(res, Err(VynkorError::InvalidPluginId(_))),
            "id {id:?} must be rejected, got {res:?}"
        );
    }

    // conn_id 1 was never consumed (all rejected pre-insert); a valid id registers
    reg.register(
        "ok.plugin-1_v2".to_string(),
        1,
        dummy_manifest(),
        dummy_write_tx(),
        "",
        "",
    )
    .expect("a well-formed id must register");
}

#[test]
fn duplicate_plugin_id_rejected() {
    let reg = PluginRegistry::new();

    reg.register(
        "dup".to_string(),
        1,
        dummy_manifest(),
        dummy_write_tx(),
        "",
        "",
    )
    .expect("first register must succeed");

    let result = reg.register(
        "dup".to_string(),
        2,
        dummy_manifest(),
        dummy_write_tx(),
        "",
        "",
    );

    assert!(
        matches!(result, Err(VynkorError::PluginAlreadyRegistered(_))),
        "expected PluginAlreadyRegistered, got {:?}",
        result
    );
}

#[test]
fn second_registration_on_same_conn_rejected_without_orphaning() {
    let reg = PluginRegistry::new();

    reg.register(
        "first".to_string(),
        7,
        dummy_manifest(),
        dummy_write_tx(),
        "",
        "",
    )
    .expect("first register must succeed");

    // same conn_id, different plugin_id — must be rejected
    let result = reg.register(
        "second".to_string(),
        7,
        dummy_manifest(),
        dummy_write_tx(),
        "",
        "",
    );
    assert!(
        matches!(result, Err(VynkorError::PluginAlreadyRegistered(_))),
        "re-registration on a live conn must be rejected, got {:?}",
        result
    );

    // "second" must not exist; "first" must still be intact and reachable by conn
    assert!(
        reg.get("second").is_none(),
        "rejected id must not be stored"
    );
    assert!(reg.get("first").is_some(), "original must survive");
    assert_eq!(
        reg.get_by_conn_id(7).map(|e| e.plugin_id.clone()),
        Some("first".to_string()),
        "conn must still map to the original plugin"
    );

    // disconnect cleans up fully — no orphan left behind
    reg.unregister("first");
    assert!(reg.get("first").is_none());
    assert!(reg.list().is_empty(), "no orphaned entries after cleanup");
}

#[test]
fn unregister_removes_from_both_indexes() {
    let reg = PluginRegistry::new();

    reg.register(
        "gone".to_string(),
        99,
        dummy_manifest(),
        dummy_write_tx(),
        "",
        "",
    )
    .unwrap();

    reg.unregister("gone");

    assert!(
        reg.get("gone").is_none(),
        "get must return None after unregister"
    );
    assert!(
        !reg.is_registered(99),
        "is_registered must return false after unregister"
    );
}

#[test]
fn list_returns_all_registered_plugins() {
    let reg = PluginRegistry::new();

    reg.register(
        "a".to_string(),
        1,
        dummy_manifest(),
        dummy_write_tx(),
        "",
        "",
    )
    .unwrap();
    reg.register(
        "b".to_string(),
        2,
        dummy_manifest(),
        dummy_write_tx(),
        "",
        "",
    )
    .unwrap();
    reg.register(
        "c".to_string(),
        3,
        dummy_manifest(),
        dummy_write_tx(),
        "",
        "",
    )
    .unwrap();

    let list = reg.list();
    assert_eq!(list.len(), 3);

    let ids: Vec<&str> = list.iter().map(|e| e.plugin_id.as_str()).collect();
    assert!(ids.contains(&"a"));
    assert!(ids.contains(&"b"));
    assert!(ids.contains(&"c"));
}

#[test]
fn is_registered_returns_true_for_known_conn_id() {
    let reg = PluginRegistry::new();

    reg.register(
        "ping".to_string(),
        77,
        dummy_manifest(),
        dummy_write_tx(),
        "",
        "",
    )
    .unwrap();

    assert!(reg.is_registered(77));
    assert!(!reg.is_registered(999));
}

#[test]
fn get_by_conn_id_returns_correct_entry() {
    let reg = PluginRegistry::new();

    reg.register(
        "alarm".to_string(),
        55,
        dummy_manifest(),
        dummy_write_tx(),
        "",
        "",
    )
    .unwrap();

    let entry = reg.get_by_conn_id(55).expect("must find by conn_id");
    assert_eq!(entry.plugin_id, "alarm");
    assert_eq!(entry.conn_id, 55);
}

#[test]
fn get_by_conn_id_returns_none_for_unknown() {
    let reg = PluginRegistry::new();
    assert!(reg.get_by_conn_id(0).is_none());
}

#[test]
fn registry_is_thread_safe() {
    let reg = Arc::new(PluginRegistry::new());
    let mut handles = vec![];

    for i in 0u64..10 {
        let reg = Arc::clone(&reg);
        handles.push(std::thread::spawn(move || {
            let id = format!("plugin_{}", i);
            let _ = reg.register(id, i, dummy_manifest(), dummy_write_tx(), "", "");
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(reg.list().len(), 10);
}

#[test]
fn concurrent_registration_of_same_plugin_id_has_exactly_one_winner() {
    // AUDIT M-08 regression: check-then-insert across by_plugin_id/by_conn_id
    // was only TOCTOU-safe because the router calls register() from a single
    // task. Hammer the same plugin_id from many threads/conn_ids concurrently
    // and assert the registry never ends up with more than one entry for it.
    let reg = Arc::new(PluginRegistry::new());
    let mut handles = vec![];

    for conn_id in 0u64..50 {
        let reg = Arc::clone(&reg);
        handles.push(std::thread::spawn(move || {
            reg.register(
                "contended".to_string(),
                conn_id,
                dummy_manifest(),
                dummy_write_tx(),
                "",
                "",
            )
            .is_ok()
        }));
    }

    let successes = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .filter(|ok| *ok)
        .count();

    assert_eq!(successes, 1, "exactly one registration must win the race");
    assert_eq!(reg.list().len(), 1);
}

#[test]
fn entry_has_registered_at_timestamp() {
    let reg = PluginRegistry::new();

    reg.register(
        "ts".to_string(),
        1,
        dummy_manifest(),
        dummy_write_tx(),
        "",
        "",
    )
    .unwrap();

    let entry = reg.get("ts").unwrap();
    // registered_at should be non-zero (Unix timestamp in seconds)
    assert!(entry.registered_at > 0, "registered_at must be set");
}

fn manifest_with_actions(actions: &[&str]) -> PluginManifest {
    PluginManifest {
        actions: actions.iter().map(|s| s.to_string()).collect(),
        ..dummy_manifest()
    }
}

#[test]
fn find_action_provider_returns_not_found_when_no_provider() {
    use vynkor::plugins::registry::ActionLookup;

    let reg = PluginRegistry::new();
    reg.register(
        "weather".to_string(),
        1,
        manifest_with_actions(&["get_forecast"]),
        dummy_write_tx(),
        "",
        "",
    )
    .unwrap();

    assert!(matches!(
        reg.find_action_provider("get_weather"),
        ActionLookup::NotFound
    ));
}

#[test]
fn find_action_provider_returns_found_for_single_provider() {
    use vynkor::plugins::registry::ActionLookup;

    let reg = PluginRegistry::new();
    reg.register(
        "weather".to_string(),
        1,
        manifest_with_actions(&["get_weather"]),
        dummy_write_tx(),
        "",
        "",
    )
    .unwrap();

    match reg.find_action_provider("get_weather") {
        ActionLookup::Found(entry) => assert_eq!(entry.plugin_id, "weather"),
        other => panic!("expected Found, got {other:?}"),
    }
}

#[test]
fn find_action_provider_returns_ambiguous_for_multiple_providers() {
    use vynkor::plugins::registry::ActionLookup;

    let reg = PluginRegistry::new();
    reg.register(
        "weather-a".to_string(),
        1,
        manifest_with_actions(&["get_weather"]),
        dummy_write_tx(),
        "",
        "",
    )
    .unwrap();
    reg.register(
        "weather-b".to_string(),
        2,
        manifest_with_actions(&["get_weather"]),
        dummy_write_tx(),
        "",
        "",
    )
    .unwrap();

    match reg.find_action_provider("get_weather") {
        ActionLookup::Ambiguous(mut ids) => {
            ids.sort();
            assert_eq!(ids, vec!["weather-a".to_string(), "weather-b".to_string()]);
        }
        other => panic!("expected Ambiguous, got {other:?}"),
    }
}

use std::time::Instant;
use vynkor::plugins::registry::PendingAction;

fn dummy_pending(original_action_id: &str, deadline: Instant) -> PendingAction {
    dummy_pending_with_provider(original_action_id, deadline, "provider")
}

fn dummy_pending_with_provider(
    original_action_id: &str,
    deadline: Instant,
    provider_id: &str,
) -> PendingAction {
    PendingAction {
        requester_write_tx: dummy_write_tx(),
        original_action_id: original_action_id.to_string(),
        requester_id: "requester".to_string(),
        deadline,
        provider_id: provider_id.to_string(),
        streaming: false,
        session_accepted: false,
        last_activity: Instant::now(),
    }
}

#[test]
fn pending_action_round_trip_take_returns_and_removes() {
    let reg = PluginRegistry::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    reg.register_pending_action("kact-1".to_string(), dummy_pending("act-1", deadline));

    let taken = reg.take_pending_action("kact-1").expect("must be present");
    assert_eq!(taken.original_action_id, "act-1");
    assert!(
        reg.take_pending_action("kact-1").is_none(),
        "must be removed after take"
    );
}

#[test]
fn pending_action_take_missing_returns_none() {
    let reg = PluginRegistry::new();
    assert!(reg.take_pending_action("does-not-exist").is_none());
}

#[test]
fn sweep_expired_actions_evicts_past_deadline_only() {
    let reg = PluginRegistry::new();
    let now = Instant::now();
    reg.register_pending_action(
        "kact-expired".to_string(),
        dummy_pending("act-expired", now - Duration::from_secs(1)),
    );
    reg.register_pending_action(
        "kact-fresh".to_string(),
        dummy_pending("act-fresh", now + Duration::from_secs(60)),
    );

    let expired = reg.sweep_expired_actions(now);
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].original_action_id, "act-expired");

    // Fresh entry must remain, expired one must be gone.
    assert!(reg.take_pending_action("kact-fresh").is_some());
    assert!(reg.take_pending_action("kact-expired").is_none());
}

#[test]
fn take_pending_action_if_provider_matching_provider_removes_it() {
    let reg = PluginRegistry::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    reg.register_pending_action(
        "kact-1".to_string(),
        dummy_pending_with_provider("act-1", deadline, "real-provider"),
    );

    let taken = reg
        .take_pending_action_if_provider("kact-1", "real-provider")
        .expect("must be present for the real provider");
    assert_eq!(taken.original_action_id, "act-1");
    assert!(
        reg.take_pending_action("kact-1").is_none(),
        "must be removed after a matching take"
    );
}

#[test]
fn take_pending_action_if_provider_mismatched_provider_leaves_it_in_place() {
    let reg = PluginRegistry::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    reg.register_pending_action(
        "kact-1".to_string(),
        dummy_pending_with_provider("act-1", deadline, "real-provider"),
    );

    // An unrelated plugin (not the routed provider) tries to claim the slot.
    assert!(
        reg.take_pending_action_if_provider("kact-1", "impostor")
            .is_none(),
        "mismatched provider must not be able to take the pending action"
    );

    // The entry must still be there for the real provider afterwards.
    let taken = reg
        .take_pending_action_if_provider("kact-1", "real-provider")
        .expect("entry must still be present for the real provider");
    assert_eq!(taken.original_action_id, "act-1");
}

fn dummy_pending_with_requester_and_provider(
    original_action_id: &str,
    deadline: Instant,
    requester_id: &str,
    provider_id: &str,
) -> PendingAction {
    PendingAction {
        requester_write_tx: dummy_write_tx(),
        original_action_id: original_action_id.to_string(),
        requester_id: requester_id.to_string(),
        deadline,
        provider_id: provider_id.to_string(),
        streaming: false,
        session_accepted: false,
        last_activity: Instant::now(),
    }
}

#[test]
fn count_pending_actions_for_counts_only_matching_requester_and_provider() {
    let reg = PluginRegistry::new();
    let deadline = Instant::now() + Duration::from_secs(30);

    // caller-a -> provider-x (2 in flight)
    reg.register_pending_action(
        "kact-1".to_string(),
        dummy_pending_with_requester_and_provider("act-1", deadline, "caller-a", "provider-x"),
    );
    reg.register_pending_action(
        "kact-2".to_string(),
        dummy_pending_with_requester_and_provider("act-2", deadline, "caller-a", "provider-x"),
    );
    // caller-a -> provider-y (different provider, must not count toward provider-x)
    reg.register_pending_action(
        "kact-3".to_string(),
        dummy_pending_with_requester_and_provider("act-3", deadline, "caller-a", "provider-y"),
    );
    // caller-b -> provider-x (different caller, must not count toward caller-a)
    reg.register_pending_action(
        "kact-4".to_string(),
        dummy_pending_with_requester_and_provider("act-4", deadline, "caller-b", "provider-x"),
    );

    assert_eq!(
        reg.count_pending_actions_for("caller-a", "provider-x"),
        2,
        "only caller-a's actions against provider-x must count"
    );
    assert_eq!(reg.count_pending_actions_for("caller-a", "provider-y"), 1);
    assert_eq!(reg.count_pending_actions_for("caller-b", "provider-x"), 1);
    assert_eq!(reg.count_pending_actions_for("caller-c", "provider-x"), 0);
}

#[test]
fn count_pending_actions_for_reflects_removal() {
    let reg = PluginRegistry::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    reg.register_pending_action(
        "kact-1".to_string(),
        dummy_pending_with_requester_and_provider("act-1", deadline, "caller-a", "provider-x"),
    );
    assert_eq!(reg.count_pending_actions_for("caller-a", "provider-x"), 1);

    reg.take_pending_action("kact-1");
    assert_eq!(
        reg.count_pending_actions_for("caller-a", "provider-x"),
        0,
        "count must drop to 0 once the pending action is taken/removed"
    );
}

#[test]
fn get_pending_action_returns_clone_without_removing() {
    let registry = PluginRegistry::new();
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    registry.register_pending_action(
        "kact-1".to_string(),
        PendingAction {
            requester_write_tx: tx,
            original_action_id: "orig-1".to_string(),
            requester_id: "caller".to_string(),
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(30),
            provider_id: "provider".to_string(),
            streaming: false,
            session_accepted: false,
            last_activity: std::time::Instant::now(),
        },
    );

    let found = registry
        .get_pending_action("kact-1")
        .expect("should find entry");
    assert_eq!(found.original_action_id, "orig-1");
    // Still present after a read-only get — take_pending_action must still work.
    assert!(registry.take_pending_action("kact-1").is_some());
}

#[test]
fn find_pending_internal_id_matches_requester_and_original_action_id() {
    let registry = PluginRegistry::new();
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    registry.register_pending_action(
        "kact-7".to_string(),
        PendingAction {
            requester_write_tx: tx,
            original_action_id: "orig-abc".to_string(),
            requester_id: "caller-x".to_string(),
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(30),
            provider_id: "provider-y".to_string(),
            streaming: false,
            session_accepted: false,
            last_activity: std::time::Instant::now(),
        },
    );

    assert_eq!(
        registry.find_pending_internal_id("caller-x", "orig-abc"),
        Some("kact-7".to_string())
    );
    // Wrong requester_id must not match, even with the right original_action_id.
    assert_eq!(
        registry.find_pending_internal_id("someone-else", "orig-abc"),
        None
    );
    // Wrong original_action_id must not match.
    assert_eq!(
        registry.find_pending_internal_id("caller-x", "not-it"),
        None
    );
}

#[test]
fn resolve_action_response_accepts_streaming_ok_without_evicting() {
    let registry = PluginRegistry::new();
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    registry.register_pending_action(
        "kact-stream".to_string(),
        PendingAction {
            requester_write_tx: tx,
            original_action_id: "orig-1".to_string(),
            requester_id: "caller".to_string(),
            deadline: Instant::now() + Duration::from_secs(30),
            provider_id: "provider".to_string(),
            streaming: true,
            session_accepted: false,
            last_activity: Instant::now(),
        },
    );

    let resolved = registry
        .resolve_action_response("kact-stream", "provider", true)
        .expect("must resolve for the matching provider");
    assert!(
        resolved.session_accepted,
        "OK response must accept the session"
    );

    // Entry must still be present (not evicted) after acceptance.
    let still_present = registry
        .get_pending_action("kact-stream")
        .expect("accepted session must not be evicted");
    assert!(still_present.session_accepted);
}

#[test]
fn resolve_action_response_evicts_streaming_error() {
    let registry = PluginRegistry::new();
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    registry.register_pending_action(
        "kact-stream-err".to_string(),
        PendingAction {
            requester_write_tx: tx,
            original_action_id: "orig-1".to_string(),
            requester_id: "caller".to_string(),
            deadline: Instant::now() + Duration::from_secs(30),
            provider_id: "provider".to_string(),
            streaming: true,
            session_accepted: false,
            last_activity: Instant::now(),
        },
    );

    let resolved = registry
        .resolve_action_response("kact-stream-err", "provider", false)
        .expect("must resolve for the matching provider");
    assert!(!resolved.session_accepted);
    assert!(
        registry.get_pending_action("kact-stream-err").is_none(),
        "a rejected streaming request must be evicted immediately"
    );
}

#[test]
fn resolve_action_response_evicts_non_streaming_ok() {
    let registry = PluginRegistry::new();
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    registry.register_pending_action(
        "kact-plain".to_string(),
        PendingAction {
            requester_write_tx: tx,
            original_action_id: "orig-1".to_string(),
            requester_id: "caller".to_string(),
            deadline: Instant::now() + Duration::from_secs(30),
            provider_id: "provider".to_string(),
            streaming: false,
            session_accepted: false,
            last_activity: Instant::now(),
        },
    );

    registry
        .resolve_action_response("kact-plain", "provider", true)
        .expect("must resolve for the matching provider");
    assert!(
        registry.get_pending_action("kact-plain").is_none(),
        "a non-streaming action must always evict on ActionResponse, OK or not"
    );
}

#[test]
fn resolve_action_response_rejects_mismatched_provider() {
    let registry = PluginRegistry::new();
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    registry.register_pending_action(
        "kact-1".to_string(),
        PendingAction {
            requester_write_tx: tx,
            original_action_id: "orig-1".to_string(),
            requester_id: "caller".to_string(),
            deadline: Instant::now() + Duration::from_secs(30),
            provider_id: "real-provider".to_string(),
            streaming: true,
            session_accepted: false,
            last_activity: Instant::now(),
        },
    );

    assert!(
        registry
            .resolve_action_response("kact-1", "impostor", true)
            .is_none(),
        "an unrelated plugin must not be able to resolve another provider's pending action"
    );
    // Entry must be untouched.
    assert!(registry.get_pending_action("kact-1").is_some());
}

#[test]
fn touch_pending_action_updates_last_activity() {
    let registry = PluginRegistry::new();
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    let old = Instant::now() - Duration::from_secs(60);
    registry.register_pending_action(
        "kact-1".to_string(),
        PendingAction {
            requester_write_tx: tx,
            original_action_id: "orig-1".to_string(),
            requester_id: "caller".to_string(),
            deadline: Instant::now() + Duration::from_secs(30),
            provider_id: "provider".to_string(),
            streaming: true,
            session_accepted: true,
            last_activity: old,
        },
    );

    registry.touch_pending_action("kact-1");

    let updated = registry.get_pending_action("kact-1").unwrap();
    assert!(
        updated.last_activity > old,
        "touch_pending_action must advance last_activity"
    );
}

#[test]
fn touch_pending_action_missing_entry_is_a_noop() {
    let registry = PluginRegistry::new();
    // Must not panic.
    registry.touch_pending_action("does-not-exist");
}

#[test]
fn sweep_idle_sessions_evicts_only_accepted_and_idle() {
    let registry = PluginRegistry::new();
    let now = Instant::now();

    let mk = |session_accepted: bool, last_activity: Instant| PendingAction {
        requester_write_tx: dummy_write_tx(),
        original_action_id: "orig".to_string(),
        requester_id: "caller".to_string(),
        deadline: now + Duration::from_secs(3600),
        provider_id: "provider".to_string(),
        streaming: true,
        session_accepted,
        last_activity,
    };

    // Accepted + idle past the bound -> swept.
    registry.register_pending_action(
        "kact-idle".to_string(),
        mk(true, now - Duration::from_secs(120)),
    );
    // Accepted + recently active -> not swept.
    registry.register_pending_action(
        "kact-active".to_string(),
        mk(true, now - Duration::from_secs(1)),
    );
    // Not yet accepted (still in the accept/reject window) -> never touched
    // by this sweep, regardless of last_activity.
    registry.register_pending_action(
        "kact-unaccepted".to_string(),
        mk(false, now - Duration::from_secs(120)),
    );

    let idle = registry.sweep_idle_sessions(now, Duration::from_secs(60));
    assert_eq!(idle.len(), 1);
    assert_eq!(idle[0].0, "kact-idle");

    assert!(registry.get_pending_action("kact-idle").is_none());
    assert!(registry.get_pending_action("kact-active").is_some());
    assert!(registry.get_pending_action("kact-unaccepted").is_some());
}

#[test]
fn sweep_expired_actions_skips_accepted_sessions() {
    let registry = PluginRegistry::new();
    let now = Instant::now();

    // Deadline already passed, but session_accepted -> must survive this sweep.
    registry.register_pending_action(
        "kact-accepted-past-deadline".to_string(),
        PendingAction {
            requester_write_tx: dummy_write_tx(),
            original_action_id: "orig".to_string(),
            requester_id: "caller".to_string(),
            deadline: now - Duration::from_secs(1),
            provider_id: "provider".to_string(),
            streaming: true,
            session_accepted: true,
            last_activity: now,
        },
    );

    let expired = registry.sweep_expired_actions(now);
    assert!(
        expired.is_empty(),
        "an accepted session must be exempt from the deadline sweep"
    );
    assert!(registry
        .get_pending_action("kact-accepted-past-deadline")
        .is_some());
}

// ── D-02: device/user identity + devices map ──────────────────────────────

#[test]
fn register_stores_declared_device_and_user() {
    let reg = PluginRegistry::new();

    reg.register(
        "weather".to_string(),
        42,
        dummy_manifest(),
        dummy_write_tx(),
        "phone-7f3a",
        "behzod",
    )
    .expect("register must succeed");

    let entry = reg.get("weather").expect("get must return entry");
    assert_eq!(entry.device_id, "phone-7f3a");
    assert_eq!(entry.user_id, "behzod");
}

#[test]
fn register_populates_devices_map() {
    let reg = PluginRegistry::new();
    let before = unix_millis_now();

    reg.register(
        "weather".to_string(),
        1,
        dummy_manifest(),
        dummy_write_tx(),
        "phone-7f3a",
        "behzod",
    )
    .expect("register must succeed");

    let dev = reg.get_device("phone-7f3a").expect("device must exist");
    assert_eq!(dev.device_id, "phone-7f3a");
    assert_eq!(dev.state, DeviceState::Online as i32);
    assert!(
        dev.last_seen >= before,
        "last_seen must be set at registration"
    );
    assert_eq!(reg.list_devices().len(), 1);
}

#[test]
fn register_upserts_existing_device_last_seen() {
    let reg = PluginRegistry::new();

    reg.register(
        "a".to_string(),
        1,
        dummy_manifest(),
        dummy_write_tx(),
        "phone-7f3a",
        "behzod",
    )
    .expect("first register must succeed");
    std::thread::sleep(Duration::from_millis(10));
    let first_seen = reg.get_device("phone-7f3a").unwrap().last_seen;

    // a second plugin from the same device must update, not duplicate
    reg.register(
        "b".to_string(),
        2,
        dummy_manifest(),
        dummy_write_tx(),
        "phone-7f3a",
        "behzod",
    )
    .expect("second register must succeed");

    assert_eq!(reg.list_devices().len(), 1, "one record per device");
    let dev = reg.get_device("phone-7f3a").unwrap();
    assert!(dev.last_seen > first_seen, "last_seen must advance");
    assert_eq!(dev.state, DeviceState::Online as i32);
}

#[test]
fn record_pong_advances_device_last_seen() {
    let reg = PluginRegistry::new();

    reg.register(
        "pinger".to_string(),
        1,
        dummy_manifest(),
        dummy_write_tx(),
        "phone-7f3a",
        "behzod",
    )
    .expect("register must succeed");

    std::thread::sleep(Duration::from_millis(10));
    let before = reg.get_device("phone-7f3a").unwrap().last_seen;

    reg.record_pong("pinger");

    let dev = reg.get_device("phone-7f3a").unwrap();
    assert!(dev.last_seen > before, "pong must advance last_seen");
    assert_eq!(dev.state, DeviceState::Online as i32);
}

#[test]
fn unregister_marks_device_offline_only_when_last_plugin_leaves() {
    let reg = PluginRegistry::new();

    reg.register(
        "a".to_string(),
        1,
        dummy_manifest(),
        dummy_write_tx(),
        "phone-7f3a",
        "behzod",
    )
    .unwrap();
    reg.register(
        "b".to_string(),
        2,
        dummy_manifest(),
        dummy_write_tx(),
        "phone-7f3a",
        "behzod",
    )
    .unwrap();

    // one plugin leaving keeps the device online — the other is still there
    reg.unregister("a");
    assert_eq!(
        reg.get_device("phone-7f3a").unwrap().state,
        DeviceState::Online as i32,
    );

    reg.unregister("b");
    assert_eq!(
        reg.get_device("phone-7f3a").unwrap().state,
        DeviceState::Offline as i32,
        "device must go offline when its last plugin leaves"
    );
}

#[test]
fn list_devices_returns_distinct_devices() {
    let reg = PluginRegistry::new();

    reg.register(
        "a".to_string(),
        1,
        dummy_manifest(),
        dummy_write_tx(),
        "host",
        "behzod",
    )
    .unwrap();
    reg.register(
        "b".to_string(),
        2,
        dummy_manifest(),
        dummy_write_tx(),
        "phone-7f3a",
        "behzod",
    )
    .unwrap();

    let mut ids: Vec<String> = reg
        .list_devices()
        .into_iter()
        .map(|d| d.device_id)
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["host".to_string(), "phone-7f3a".to_string()]);
}

fn unix_millis_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ── D-03: device metadata off the wire ─────────────────────────────────────

#[test]
fn register_with_device_stores_metadata() {
    let reg = PluginRegistry::new();

    reg.register_with_device(
        "geo".to_string(),
        1,
        dummy_manifest(),
        dummy_write_tx(),
        DeviceMeta {
            device_id: "phone-7f3a".to_string(),
            user_id: "behzod".to_string(),
            os: DeviceOs::Android,
            arch: "aarch64".to_string(),
            os_version: "14".to_string(),
            capabilities: vec!["geo".to_string(), "battery".to_string()],
        },
    )
    .expect("register must succeed");

    let dev = reg.get_device("phone-7f3a").expect("device must exist");
    assert_eq!(dev.os, DeviceOs::Android as i32);
    assert_eq!(dev.arch, "aarch64");
    assert_eq!(dev.os_version, "14");
    assert_eq!(
        dev.capabilities,
        vec!["geo".to_string(), "battery".to_string()]
    );

    let entry = reg.get("geo").expect("plugin must exist");
    assert_eq!(entry.device_id, "phone-7f3a");
    assert_eq!(entry.user_id, "behzod");
}

#[test]
fn register_with_device_defaults_empty_identity() {
    let reg = PluginRegistry::new();

    reg.register_with_device(
        "host-plugin".to_string(),
        1,
        dummy_manifest(),
        dummy_write_tx(),
        DeviceMeta::default(),
    )
    .expect("register must succeed");

    let entry = reg.get("host-plugin").expect("plugin must exist");
    assert_eq!(entry.device_id, "local");
    assert_eq!(entry.user_id, "default");
}

#[test]
fn re_register_refreshes_device_metadata() {
    let reg = PluginRegistry::new();

    reg.register_with_device(
        "a".to_string(),
        1,
        dummy_manifest(),
        dummy_write_tx(),
        DeviceMeta {
            device_id: "phone-7f3a".to_string(),
            os: DeviceOs::Android,
            capabilities: vec!["geo".to_string()],
            ..Default::default()
        },
    )
    .expect("first register must succeed");

    reg.register_with_device(
        "b".to_string(),
        2,
        dummy_manifest(),
        dummy_write_tx(),
        DeviceMeta {
            device_id: "phone-7f3a".to_string(),
            os: DeviceOs::Linux,
            capabilities: vec!["clipboard".to_string()],
            ..Default::default()
        },
    )
    .expect("second register must succeed");

    let dev = reg.get_device("phone-7f3a").expect("device must exist");
    assert_eq!(
        dev.os,
        DeviceOs::Linux as i32,
        "metadata must refresh on re-register"
    );
    assert_eq!(dev.capabilities, vec!["clipboard".to_string()]);
    assert_eq!(reg.list_devices().len(), 1, "still one device record");
}

#[test]
fn register_single_device_with_multiple_caps_creates_one_entry() {
    let reg = PluginRegistry::new();
    let caps = vec![
        "battery".to_string(),
        "geo".to_string(),
        "clipboard".to_string(),
        "contacts".to_string(),
        "mic".to_string(),
        "speaker".to_string(),
        "chat".to_string(),
        "device".to_string(),
    ];
    reg.register_device(
        "dev-xxx".to_string(),
        99,
        caps.clone(),
        dummy_manifest(),
        dummy_write_tx(),
        DeviceMeta {
            device_id: "dev-xxx".to_string(),
            user_id: "default".to_string(),
            os: DeviceOs::Android,
            arch: "aarch64".to_string(),
            os_version: "14".to_string(),
            capabilities: caps.clone(),
        },
    )
    .expect("mux register must succeed");

    assert_eq!(reg.list().len(), 1, "one registry entry for mux device");
    let entry = reg.get("dev-xxx").expect("device entry must exist");
    assert_eq!(entry.plugin_id, "dev-xxx");
    assert_eq!(entry.conn_id, 99);
    let expected_actions: Vec<String> = caps.iter().map(|c| format!("dev-xxx.{c}")).collect();
    assert_eq!(entry.manifest.actions, expected_actions);
    let dev = reg.get_device("dev-xxx").expect("device info must exist");
    assert_eq!(dev.capabilities, caps);
    assert_eq!(reg.get_by_conn_id(99).unwrap().plugin_id, "dev-xxx");
    assert!(reg.get_mux("dev-xxx.geo").is_some());
    assert_eq!(reg.get_mux("dev-xxx.geo").unwrap().plugin_id, "dev-xxx");
    assert!(reg.get_mux("dev-xxx.battery").is_some());
}

#[test]
fn deprecated_per_cap_still_works() {
    let reg = PluginRegistry::new();
    for (i, cap) in ["geo", "battery", "clipboard"].iter().enumerate() {
        let plugin_id = format!("dev-xxx.{cap}");
        reg.register_with_device(
            plugin_id.clone(),
            i as u64 + 1,
            dummy_manifest(),
            dummy_write_tx(),
            DeviceMeta {
                device_id: "dev-xxx".to_string(),
                user_id: "default".to_string(),
                os: DeviceOs::Android,
                arch: "aarch64".to_string(),
                os_version: "14".to_string(),
                capabilities: vec![cap.to_string()],
            },
        )
        .expect("per-cap register must succeed");
    }
    assert_eq!(reg.list().len(), 3);
    assert!(reg.get("dev-xxx.geo").is_some());
    assert!(reg.get("dev-xxx.battery").is_some());
    assert!(reg.get("dev-xxx.clipboard").is_some());
    let dev = reg.get_device("dev-xxx").expect("device must exist");
    assert_eq!(dev.capabilities, vec!["clipboard".to_string()]);
}

#[test]
fn get_mux_fallback_resolves_device_prefix() {
    let reg = PluginRegistry::new();
    let caps = vec!["geo".to_string(), "battery".to_string()];
    reg.register_device(
        "dev-123".to_string(),
        10,
        caps.clone(),
        dummy_manifest(),
        dummy_write_tx(),
        DeviceMeta {
            device_id: "dev-123".to_string(),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        reg.get("dev-123.geo").is_none(),
        "exact per-cap must not exist"
    );
    let mux = reg
        .get_mux("dev-123.geo")
        .expect("mux fallback must find device");
    assert_eq!(mux.plugin_id, "dev-123");
    assert!(
        reg.get_mux("dev-123.unknown").is_none(),
        "unknown cap must not resolve"
    );
    assert!(reg.get_mux("unknown").is_none());
}
