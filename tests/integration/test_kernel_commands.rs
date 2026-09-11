use super::helpers::{start_kernel, start_kernel_with_config, test_config};
use std::time::{Duration, Instant};
use tokio::time::timeout;
use vynkor::proto::vynkor::{ActionStatus, CommandStatus, PluginManifest};

fn admin_manifest() -> PluginManifest {
    PluginManifest {
        permissions: vec!["PERMISSION_KERNEL_ADMIN".to_string()],
        ..Default::default()
    }
}
use vynkor_sdk::VynkorClient;

#[tokio::test]
async fn health_check_via_ipc_returns_ok_with_json_fields() {
    let (shutdown_tx, _registry, _bus) = start_kernel("/tmp/vynkor_integ_cmd_hc.sock", 19210).await;

    let mut client = VynkorClient::connect("/tmp/vynkor_integ_cmd_hc.sock")
        .await
        .unwrap();
    client
        .register("cmd-hc-client", PluginManifest::default())
        .await
        .unwrap();

    let ack = timeout(
        Duration::from_secs(2),
        client.send_command("cmd-1", "health_check", b"{}"),
    )
    .await
    .expect("timed out")
    .expect("send_command failed");

    assert_eq!(ack.status(), CommandStatus::CommandOk);
    assert!(ack.error.is_empty(), "unexpected error: {}", ack.error);
    let json = String::from_utf8(ack.data_json).unwrap();
    assert!(
        json.contains("uptime_secs"),
        "missing uptime_secs in: {json}"
    );
    assert!(
        json.contains("plugin_count"),
        "missing plugin_count in: {json}"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn kernel_command_ack_echoes_command_id() {
    let (shutdown_tx, _registry, _bus) = start_kernel("/tmp/vynkor_integ_cmd_id.sock", 19211).await;

    let mut client = VynkorClient::connect("/tmp/vynkor_integ_cmd_id.sock")
        .await
        .unwrap();
    client
        .register("cmd-id-client", PluginManifest::default())
        .await
        .unwrap();

    let ack = timeout(
        Duration::from_secs(2),
        client.send_command("unique-id-42", "health_check", b""),
    )
    .await
    .expect("timed out")
    .expect("send_command failed");

    assert_eq!(ack.command_id, "unique-id-42");

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn unknown_command_via_ipc_returns_command_unknown() {
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_cmd_unk.sock", 19212).await;

    let mut client = VynkorClient::connect("/tmp/vynkor_integ_cmd_unk.sock")
        .await
        .unwrap();
    client
        .register("cmd-unk-client", admin_manifest())
        .await
        .unwrap();

    let ack = timeout(
        Duration::from_secs(2),
        client.send_command("x1", "totally_unknown_cmd", b""),
    )
    .await
    .expect("timed out")
    .expect("send_command failed");

    assert_eq!(ack.status(), CommandStatus::CommandUnknown);
    assert!(
        ack.error.contains("totally_unknown_cmd"),
        "error should name the command, got: {}",
        ack.error
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn reload_config_without_path_returns_error_via_ipc() {
    // Kernel started without a config_file path → reload must return COMMAND_ERROR
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_cmd_reload.sock", 19213).await;

    let mut client = VynkorClient::connect("/tmp/vynkor_integ_cmd_reload.sock")
        .await
        .unwrap();
    client
        .register("cmd-reload-client", admin_manifest())
        .await
        .unwrap();

    let ack = timeout(
        Duration::from_secs(2),
        client.send_command("r1", "reload_config", b""),
    )
    .await
    .expect("timed out")
    .expect("send_command failed");

    assert_eq!(ack.status(), CommandStatus::CommandError);
    assert!(
        ack.error.contains("no config path"),
        "expected 'no config path', got: {}",
        ack.error
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn reload_config_without_admin_permission_is_denied() {
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_cmd_reload_denied.sock", 19214).await;

    let mut client = VynkorClient::connect("/tmp/vynkor_integ_cmd_reload_denied.sock")
        .await
        .unwrap();
    client
        .register("cmd-reload-denied-client", PluginManifest::default())
        .await
        .unwrap();

    let ack = timeout(
        Duration::from_secs(2),
        client.send_command("r2", "reload_config", b""),
    )
    .await
    .expect("timed out")
    .expect("send_command failed");

    assert_eq!(ack.status(), CommandStatus::CommandPermissionDenied);
    assert!(
        ack.error.contains("PERMISSION_KERNEL_ADMIN"),
        "expected PERMISSION_KERNEL_ADMIN in error, got: {}",
        ack.error
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn health_check_exempt_from_admin_permission() {
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_cmd_hc_noauth.sock", 19215).await;

    let mut client = VynkorClient::connect("/tmp/vynkor_integ_cmd_hc_noauth.sock")
        .await
        .unwrap();
    client
        .register("cmd-hc-noauth-client", PluginManifest::default())
        .await
        .unwrap();

    let ack = timeout(
        Duration::from_secs(2),
        client.send_command("hc-1", "health_check", b"{}"),
    )
    .await
    .expect("timed out")
    .expect("send_command failed");

    assert_eq!(ack.status(), CommandStatus::CommandOk);

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn manifest_discovery_commands_exempt_from_admin_permission() {
    // The agent's tool-discovery surface: list_plugins + get_manifest must
    // work for a plain plugin (no KERNEL_ADMIN), while reload_config stays
    // denied for the same sender.
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_cmd_discovery.sock", 19217).await;

    let mut client = VynkorClient::connect("/tmp/vynkor_integ_cmd_discovery.sock")
        .await
        .unwrap();
    client
        .register("cmd-discovery-client", PluginManifest::default())
        .await
        .unwrap();

    let ack = timeout(
        Duration::from_secs(2),
        client.send_command("d1", "list_plugins", b"{}"),
    )
    .await
    .expect("timed out")
    .expect("send_command failed");
    assert_eq!(
        ack.status(),
        CommandStatus::CommandOk,
        "list_plugins should be exempt, got error: {}",
        ack.error
    );

    // get_manifest for an unknown slug reaches the handler (CommandError
    // naming the plugin) instead of being permission-denied — that's the
    // exemption working; a gated command would never see params.
    let ack = timeout(
        Duration::from_secs(2),
        client.send_command("d2", "get_manifest", br#"{"plugin_id":"whatever"}"#),
    )
    .await
    .expect("timed out")
    .expect("send_command failed");
    assert_ne!(
        ack.status(),
        CommandStatus::CommandPermissionDenied,
        "get_manifest must be exempt from KERNEL_ADMIN"
    );
    assert!(
        ack.error.contains("plugin not registered"),
        "error was: {}",
        ack.error
    );

    let denied = timeout(
        Duration::from_secs(2),
        client.send_command("d3", "reload_config", b""),
    )
    .await
    .expect("timed out")
    .expect("send_command failed");
    assert_eq!(denied.status(), CommandStatus::CommandPermissionDenied);

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn kernel_targeted_action_request_returns_not_found_not_fake_ok() {
    // R5-07 interim honesty fix: the kernel's ActionRequest handler is a
    // permission-check-only stub — it never routes to a provider or executes
    // anything. Reporting ACTION_OK after a passing permission check would
    // lie to callers about work that never happened (AUDIT H-05).
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_cmd_action_stub.sock", 19216).await;

    let mut client = VynkorClient::connect("/tmp/vynkor_integ_cmd_action_stub.sock")
        .await
        .unwrap();
    client
        .register(
            "action-stub-client",
            PluginManifest {
                permissions: vec!["PERMISSION_SYSTEM".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let resp = timeout(
        Duration::from_secs(2),
        client.send_action("get_cpu", b"{}", 2000),
    )
    .await
    .expect("timed out")
    .expect("send_action failed");

    assert_eq!(
        resp.status,
        ActionStatus::ActionNotFound as i32,
        "kernel has no action executor yet — must not claim ACTION_OK"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn kernel_routes_action_to_declared_provider_and_correlates_response() {
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_action_route.sock", 19217).await;

    // Provider registers first and declares the action.
    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_action_route.sock")
        .await
        .unwrap();
    provider
        .register(
            "weather-provider",
            PluginManifest {
                actions: vec!["get_weather".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut requester = VynkorClient::connect("/tmp/vynkor_integ_action_route.sock")
        .await
        .unwrap();
    requester
        .register("weather-requester", PluginManifest::default())
        .await
        .unwrap();

    // Requester fires the action at "kernel" (existing SDK API, unaware of routing).
    // Spawned so the request is actually sent now: an `async fn` call produces
    // a lazy future that does nothing until polled, so binding it without
    // driving it would leave the provider waiting on a request that was never
    // written to the socket.
    let request_fut = tokio::spawn(async move {
        requester
            .send_action("get_weather", br#"{"city":"nyc"}"#, 2000)
            .await
    });

    // Provider receives the routed request and answers OK, targeted at "kernel".
    let received = timeout(Duration::from_secs(2), provider.recv())
        .await
        .expect("provider recv timed out")
        .expect("provider recv failed");
    let internal_action_id = match received.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(req)) => {
            assert_eq!(req.action, "get_weather");
            assert_eq!(req.params_json, br#"{"city":"nyc"}"#);
            req.action_id
        }
        other => panic!("expected ActionRequest, got {other:?}"),
    };

    let resp_env = vynkor::proto::vynkor::Envelope {
        payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(
            vynkor::proto::vynkor::ActionResponse {
                action_id: internal_action_id,
                status: ActionStatus::ActionOk as i32,
                data_json: br#"{"temp_f":72}"#.to_vec(),
                error: String::new(),
            },
        )),
        ..Default::default()
    };
    provider.send("kernel", resp_env).await.unwrap();

    let resp = timeout(Duration::from_secs(2), request_fut)
        .await
        .expect("timed out")
        .expect("task panicked")
        .expect("send_action failed");

    assert_eq!(resp.status, ActionStatus::ActionOk as i32);
    assert_eq!(resp.data_json, br#"{"temp_f":72}"#);

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn kernel_denies_action_when_provider_lacks_required_permission() {
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_action_perm_deny.sock", 19218).await;

    // Provider declares the action but not the permission it requires
    // (http_request -> PERMISSION_NETWORK, see auth::permissions::required_permission_for_action).
    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_action_perm_deny.sock")
        .await
        .unwrap();
    provider
        .register(
            "network-imposter",
            PluginManifest {
                actions: vec!["http_request".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut requester = VynkorClient::connect("/tmp/vynkor_integ_action_perm_deny.sock")
        .await
        .unwrap();
    requester
        .register("action-requester", PluginManifest::default())
        .await
        .unwrap();

    let resp = timeout(
        Duration::from_secs(2),
        requester.send_action("http_request", br#"{"url":"http://example.com"}"#, 2000),
    )
    .await
    .expect("timed out")
    .expect("send_action failed");

    assert_eq!(
        resp.status,
        ActionStatus::ActionPermissionDeny as i32,
        "provider without PERMISSION_NETWORK must not receive http_request"
    );

    // The provider must never have been forwarded the request.
    let never_received = timeout(Duration::from_millis(300), provider.recv()).await;
    assert!(
        never_received.is_err(),
        "provider without required permission must not receive the ActionRequest"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn kernel_denies_action_when_requester_lacks_required_permission() {
    // T-19: even when the provider legitimately holds PERMISSION_NETWORK, an
    // unprivileged requester must not be able to launder a network request
    // through it by calling the declared action directly.
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_action_perm_deny_requester.sock", 19219).await;

    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_action_perm_deny_requester.sock")
        .await
        .unwrap();
    provider
        .register(
            "network-provider",
            PluginManifest {
                actions: vec!["http_request".to_string()],
                permissions: vec!["PERMISSION_NETWORK".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut requester = VynkorClient::connect("/tmp/vynkor_integ_action_perm_deny_requester.sock")
        .await
        .unwrap();
    requester
        .register("unprivileged-requester", PluginManifest::default())
        .await
        .unwrap();

    let resp = timeout(
        Duration::from_secs(2),
        requester.send_action("http_request", br#"{"url":"http://example.com"}"#, 2000),
    )
    .await
    .expect("timed out")
    .expect("send_action failed");

    assert_eq!(
        resp.status,
        ActionStatus::ActionPermissionDeny as i32,
        "requester without PERMISSION_NETWORK must not be able to invoke http_request \
         even via a provider that legitimately holds the permission"
    );

    let never_received = timeout(Duration::from_millis(300), provider.recv()).await;
    assert!(
        never_received.is_err(),
        "provider must never receive the ActionRequest when the requester lacks the permission"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn kernel_enforces_provider_declared_per_action_permission() {
    // Manifest v2: the provider-declared per-action permission gates callers at
    // routing time (data-driven anti-laundering). `db_get` requires
    // PERMISSION_STORAGE on BOTH provider and requester; a requester without it
    // is denied, a requester with it is routed through to the provider.
    use std::collections::HashMap;
    use vynkor::proto::vynkor::PermissionType;

    let (shutdown_tx, registry, _bus) =
        start_kernel("/tmp/vynkor_integ_action_v2_perm.sock", 19234).await;

    // Provider declares the action AND holds the required permission.
    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_action_v2_perm.sock")
        .await
        .unwrap();
    provider
        .register(
            "db-provider",
            PluginManifest {
                actions: vec!["db_get".to_string()],
                permissions: vec!["PERMISSION_STORAGE".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    registry.set_action_requirements(
        "db-provider".to_string(),
        HashMap::from([("db_get".to_string(), PermissionType::PermissionStorage)]),
    );

    // Half 1: requester WITHOUT PERMISSION_STORAGE is denied.
    let mut unprivileged = VynkorClient::connect("/tmp/vynkor_integ_action_v2_perm.sock")
        .await
        .unwrap();
    unprivileged
        .register("unprivileged-requester", PluginManifest::default())
        .await
        .unwrap();

    let resp = timeout(
        Duration::from_secs(2),
        unprivileged.send_action("db_get", br#"{"key":"a"}"#, 2000),
    )
    .await
    .expect("timed out")
    .expect("send_action failed");
    assert_eq!(
        resp.status,
        ActionStatus::ActionPermissionDeny as i32,
        "requester without PERMISSION_STORAGE must be denied"
    );
    let never_received = timeout(Duration::from_millis(300), provider.recv()).await;
    assert!(
        never_received.is_err(),
        "provider must not receive the denied request"
    );

    // Half 2: requester WITH PERMISSION_STORAGE is routed through.
    let mut privileged = VynkorClient::connect("/tmp/vynkor_integ_action_v2_perm.sock")
        .await
        .unwrap();
    privileged
        .register(
            "privileged-requester",
            PluginManifest {
                permissions: vec!["PERMISSION_STORAGE".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let request_fut = tokio::spawn(async move {
        privileged
            .send_action("db_get", br#"{"key":"a"}"#, 2000)
            .await
    });

    let received = timeout(Duration::from_secs(2), provider.recv())
        .await
        .expect("provider recv timed out")
        .expect("provider recv failed");
    let internal_action_id = match received.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(req)) => {
            assert_eq!(req.action, "db_get");
            req.action_id
        }
        other => panic!("expected ActionRequest, got {other:?}"),
    };

    let resp_env = vynkor::proto::vynkor::Envelope {
        payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(
            vynkor::proto::vynkor::ActionResponse {
                action_id: internal_action_id,
                status: ActionStatus::ActionOk as i32,
                data_json: br#"{"value":"v"}"#.to_vec(),
                error: String::new(),
            },
        )),
        ..Default::default()
    };
    provider.send("kernel", resp_env).await.unwrap();

    let resp = timeout(Duration::from_secs(2), request_fut)
        .await
        .expect("timed out")
        .expect("task panicked")
        .expect("send_action failed");
    assert_eq!(
        resp.status,
        ActionStatus::ActionOk as i32,
        "requester with PERMISSION_STORAGE must be routed to the provider"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn action_concurrency_cap_denies_third_concurrent_call_to_same_provider() {
    // R6-03: a caller with action_caller_max_concurrent = 2 gets a 3rd concurrent
    // ActionRequest to the SAME provider denied, but a concurrent request to a
    // DIFFERENT provider still succeeds — proves per-(caller, provider) keying.
    let mut cfg = test_config("/tmp/vynkor_integ_action_concurrency_cap.sock", 19230);
    cfg.action_caller_max_concurrent = Some(2);
    let (shutdown_tx, _registry, _bus) = start_kernel_with_config(cfg).await;

    let mut provider_x = VynkorClient::connect("/tmp/vynkor_integ_action_concurrency_cap.sock")
        .await
        .unwrap();
    provider_x
        .register(
            "provider-x",
            PluginManifest {
                actions: vec!["slow_action".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut provider_y = VynkorClient::connect("/tmp/vynkor_integ_action_concurrency_cap.sock")
        .await
        .unwrap();
    provider_y
        .register(
            "provider-y",
            PluginManifest {
                actions: vec!["other_action".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut caller = VynkorClient::connect("/tmp/vynkor_integ_action_concurrency_cap.sock")
        .await
        .unwrap();
    caller
        .register("caller-a", PluginManifest::default())
        .await
        .unwrap();

    // Fire 2 raw ActionRequests to provider-x without waiting for a response —
    // provider-x never replies to these, so both stay pending and fill the cap.
    for i in 0..2 {
        let env = vynkor::proto::vynkor::Envelope {
            payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(
                vynkor::proto::vynkor::ActionRequest {
                    action_id: format!("fill-{i}"),
                    action: "slow_action".to_string(),
                    params_json: b"{}".to_vec(),
                    timeout_ms: 5000,
                    streaming: false,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        caller.send("kernel", env).await.unwrap();
    }

    // A 3rd ActionRequest to the SAME provider must be denied immediately —
    // the kernel never forwards it, so no provider recv() is needed here.
    let deny_env = vynkor::proto::vynkor::Envelope {
        payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(
            vynkor::proto::vynkor::ActionRequest {
                action_id: "act-3".to_string(),
                action: "slow_action".to_string(),
                params_json: b"{}".to_vec(),
                timeout_ms: 5000,
                streaming: false,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    caller.send("kernel", deny_env).await.unwrap();

    let deny_resp = timeout(Duration::from_secs(2), caller.recv())
        .await
        .expect("must not hang — denial is synchronous")
        .expect("recv failed");
    match deny_resp.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(resp)) => {
            assert_eq!(resp.action_id, "act-3");
            assert_eq!(
                resp.status,
                ActionStatus::ActionQuotaExceeded as i32,
                "3rd concurrent action to the same provider must be denied once cap is reached"
            );
        }
        other => panic!("expected ActionResponse, got {other:?}"),
    }

    // A request to the DIFFERENT provider (provider-y) must still succeed —
    // proves the cap is per-(caller, provider), not global.
    let other_env = vynkor::proto::vynkor::Envelope {
        payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(
            vynkor::proto::vynkor::ActionRequest {
                action_id: "act-4".to_string(),
                action: "other_action".to_string(),
                params_json: b"{}".to_vec(),
                timeout_ms: 2000,
                streaming: false,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    caller.send("kernel", other_env).await.unwrap();

    let received = timeout(Duration::from_secs(2), provider_y.recv())
        .await
        .expect("provider-y recv timed out")
        .expect("provider-y recv failed");
    let internal_action_id = match received.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(req)) => {
            assert_eq!(req.action, "other_action");
            req.action_id
        }
        other => panic!("expected ActionRequest, got {other:?}"),
    };
    let resp_env = vynkor::proto::vynkor::Envelope {
        payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(
            vynkor::proto::vynkor::ActionResponse {
                action_id: internal_action_id,
                status: ActionStatus::ActionOk as i32,
                data_json: b"{}".to_vec(),
                error: String::new(),
            },
        )),
        ..Default::default()
    };
    provider_y.send("kernel", resp_env).await.unwrap();

    let resp_y = timeout(Duration::from_secs(2), caller.recv())
        .await
        .expect("must not hang")
        .expect("recv failed");
    match resp_y.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(resp)) => {
            assert_eq!(resp.action_id, "act-4");
            assert_eq!(
                resp.status,
                ActionStatus::ActionOk as i32,
                "a different provider must be unaffected by the caller's cap against provider-x"
            );
        }
        other => panic!("expected ActionResponse, got {other:?}"),
    }

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn action_concurrency_cap_releases_after_response_allowing_retry() {
    // R6-03: proves the release path end-to-end, not just that the cap denies.
    // With action_caller_max_concurrent = 1, a 2nd concurrent request to the
    // same provider is denied while the 1st is still in flight. Once the
    // provider answers the 1st request, the slot frees up with no explicit
    // decrement anywhere — the cap is a live scan of pending actions — and a
    // 3rd request from the same caller to the same provider must then route
    // through successfully.
    let mut cfg = test_config(
        "/tmp/vynkor_integ_action_concurrency_cap_release.sock",
        19233,
    );
    cfg.action_caller_max_concurrent = Some(1);
    let (shutdown_tx, _registry, _bus) = start_kernel_with_config(cfg).await;

    let mut provider =
        VynkorClient::connect("/tmp/vynkor_integ_action_concurrency_cap_release.sock")
            .await
            .unwrap();
    provider
        .register(
            "release-provider",
            PluginManifest {
                actions: vec!["slow_action".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut caller = VynkorClient::connect("/tmp/vynkor_integ_action_concurrency_cap_release.sock")
        .await
        .unwrap();
    caller
        .register("release-caller", PluginManifest::default())
        .await
        .unwrap();

    // Fire a raw ActionRequest that fills the (caller, provider) cap of 1.
    // The provider does not reply yet, so it stays pending.
    let fill_env = vynkor::proto::vynkor::Envelope {
        payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(
            vynkor::proto::vynkor::ActionRequest {
                action_id: "fill-1".to_string(),
                action: "slow_action".to_string(),
                params_json: b"{}".to_vec(),
                timeout_ms: 5000,
                streaming: false,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    caller.send("kernel", fill_env).await.unwrap();

    // A 2nd request to the same provider must be denied immediately — the
    // cap is already full and the kernel never forwards it to the provider.
    let denied_env = vynkor::proto::vynkor::Envelope {
        payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(
            vynkor::proto::vynkor::ActionRequest {
                action_id: "denied-1".to_string(),
                action: "slow_action".to_string(),
                params_json: b"{}".to_vec(),
                timeout_ms: 5000,
                streaming: false,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    caller.send("kernel", denied_env).await.unwrap();

    let denied_resp = timeout(Duration::from_secs(2), caller.recv())
        .await
        .expect("must not hang — denial is synchronous")
        .expect("recv failed");
    match denied_resp.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(resp)) => {
            assert_eq!(resp.action_id, "denied-1");
            assert_eq!(
                resp.status,
                ActionStatus::ActionQuotaExceeded as i32,
                "2nd concurrent action to the same provider must be denied once cap of 1 is reached"
            );
        }
        other => panic!("expected ActionResponse, got {other:?}"),
    }

    // Now free the slot: the provider receives fill-1's routed request and
    // replies OK. The caller must then receive fill-1's response.
    let received = timeout(Duration::from_secs(2), provider.recv())
        .await
        .expect("provider recv timed out")
        .expect("provider recv failed");
    let internal_action_id = match received.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(req)) => {
            assert_eq!(req.action, "slow_action");
            req.action_id
        }
        other => panic!("expected ActionRequest, got {other:?}"),
    };
    let fill_resp_env = vynkor::proto::vynkor::Envelope {
        payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(
            vynkor::proto::vynkor::ActionResponse {
                action_id: internal_action_id,
                status: ActionStatus::ActionOk as i32,
                data_json: b"{}".to_vec(),
                error: String::new(),
            },
        )),
        ..Default::default()
    };
    provider.send("kernel", fill_resp_env).await.unwrap();

    let fill_caller_resp = timeout(Duration::from_secs(2), caller.recv())
        .await
        .expect("must not hang")
        .expect("recv failed");
    match fill_caller_resp.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(resp)) => {
            assert_eq!(resp.action_id, "fill-1");
            assert_eq!(resp.status, ActionStatus::ActionOk as i32);
        }
        other => panic!("expected ActionResponse, got {other:?}"),
    }

    // The slot is now free (no explicit decrement — the cap re-scans
    // pending actions on every request). A 3rd request from the SAME
    // caller to the SAME provider must now route through successfully.
    let retry_env = vynkor::proto::vynkor::Envelope {
        payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(
            vynkor::proto::vynkor::ActionRequest {
                action_id: "retry-1".to_string(),
                action: "slow_action".to_string(),
                params_json: b"{}".to_vec(),
                timeout_ms: 5000,
                streaming: false,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    caller.send("kernel", retry_env).await.unwrap();

    let retry_received = timeout(Duration::from_secs(2), provider.recv())
        .await
        .expect("provider recv timed out on retry")
        .expect("provider recv failed on retry");
    let retry_internal_action_id = match retry_received.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(req)) => {
            assert_eq!(req.action, "slow_action");
            req.action_id
        }
        other => panic!("expected ActionRequest, got {other:?}"),
    };
    let retry_resp_env = vynkor::proto::vynkor::Envelope {
        payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(
            vynkor::proto::vynkor::ActionResponse {
                action_id: retry_internal_action_id,
                status: ActionStatus::ActionOk as i32,
                data_json: b"{}".to_vec(),
                error: String::new(),
            },
        )),
        ..Default::default()
    };
    provider.send("kernel", retry_resp_env).await.unwrap();

    let retry_caller_resp = timeout(Duration::from_secs(2), caller.recv())
        .await
        .expect("must not hang")
        .expect("recv failed");
    match retry_caller_resp.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(resp)) => {
            assert_eq!(resp.action_id, "retry-1");
            assert_eq!(
                resp.status,
                ActionStatus::ActionOk as i32,
                "retry after the in-flight action completed must succeed — proves the \
                 scan-based cap self-corrects with no explicit decrement"
            );
        }
        other => panic!("expected ActionResponse, got {other:?}"),
    }

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn action_rate_limit_denies_burst_above_configured_rps() {
    // R6-03: with action_caller_rate_limit_rps = 1, a rapid second request from
    // the same (caller, provider) within the same second is denied.
    let mut cfg = test_config("/tmp/vynkor_integ_action_rate_limit.sock", 19231);
    cfg.action_caller_rate_limit_rps = Some(1);
    let (shutdown_tx, _registry, _bus) = start_kernel_with_config(cfg).await;

    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_action_rate_limit.sock")
        .await
        .unwrap();
    provider
        .register(
            "rl-provider",
            PluginManifest {
                actions: vec!["ping_action".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut caller = VynkorClient::connect("/tmp/vynkor_integ_action_rate_limit.sock")
        .await
        .unwrap();
    caller
        .register("rl-caller", PluginManifest::default())
        .await
        .unwrap();

    // First request: routes through fine (rps=1 allows one immediately).
    let request_fut = tokio::spawn(async move {
        let resp = caller
            .send_action("ping_action", b"{}", 2000)
            .await
            .unwrap();
        (caller, resp)
    });

    let received = timeout(Duration::from_secs(2), provider.recv())
        .await
        .expect("provider recv timed out")
        .expect("provider recv failed");
    let internal_action_id = match received.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(req)) => req.action_id,
        other => panic!("expected ActionRequest, got {other:?}"),
    };
    let resp_env = vynkor::proto::vynkor::Envelope {
        payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(
            vynkor::proto::vynkor::ActionResponse {
                action_id: internal_action_id,
                status: ActionStatus::ActionOk as i32,
                data_json: b"{}".to_vec(),
                error: String::new(),
            },
        )),
        ..Default::default()
    };
    provider.send("kernel", resp_env).await.unwrap();

    let (mut caller, first) = timeout(Duration::from_secs(2), request_fut)
        .await
        .expect("timed out")
        .expect("task panicked");
    assert_eq!(first.status, ActionStatus::ActionOk as i32);

    // Immediately send a second request — with rps=1 the bucket should be empty.
    let second = timeout(
        Duration::from_secs(2),
        caller.send_action("ping_action", b"{}", 2000),
    )
    .await
    .expect("must not hang")
    .expect("send_action failed");
    assert_eq!(
        second.status,
        ActionStatus::ActionQuotaExceeded as i32,
        "immediate second request must be denied by the rps=1 limiter"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn action_quota_unset_leaves_routing_unlimited() {
    // R6-03: with both quota configs left at their None default, action routing
    // behaves exactly as before this feature (regression guard for the opt-in
    // convention).
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_action_quota_unset.sock", 19232).await;

    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_action_quota_unset.sock")
        .await
        .unwrap();
    provider
        .register(
            "unlimited-provider",
            PluginManifest {
                actions: vec!["ping_action".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut caller = VynkorClient::connect("/tmp/vynkor_integ_action_quota_unset.sock")
        .await
        .unwrap();
    caller
        .register("unlimited-caller", PluginManifest::default())
        .await
        .unwrap();

    for i in 0..5 {
        let request_fut = tokio::spawn(async move {
            let resp = caller
                .send_action("ping_action", b"{}", 2000)
                .await
                .unwrap();
            (caller, resp)
        });

        let received = timeout(Duration::from_secs(2), provider.recv())
            .await
            .unwrap_or_else(|_| panic!("provider recv timed out on iteration {i}"))
            .expect("provider recv failed");
        let internal_action_id = match received.payload {
            Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(req)) => req.action_id,
            other => panic!("expected ActionRequest, got {other:?}"),
        };
        let resp_env = vynkor::proto::vynkor::Envelope {
            payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(
                vynkor::proto::vynkor::ActionResponse {
                    action_id: internal_action_id,
                    status: ActionStatus::ActionOk as i32,
                    data_json: b"{}".to_vec(),
                    error: String::new(),
                },
            )),
            ..Default::default()
        };
        provider.send("kernel", resp_env).await.unwrap();

        let (c, resp) = timeout(Duration::from_secs(2), request_fut)
            .await
            .unwrap_or_else(|_| panic!("timed out on iteration {i}"))
            .expect("task panicked");
        caller = c;
        assert_eq!(
            resp.status,
            ActionStatus::ActionOk as i32,
            "with no quota configured, no request should ever be denied (iteration {i})"
        );
    }

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn action_response_from_non_provider_plugin_is_rejected_not_proxied() {
    // AUDIT (Critical): the internal correlation id handed to the provider
    // is minted from a global, monotonic, zero-entropy counter
    // (`kact-<n>`), so it's trivially predictable/observable by any other
    // registered plugin. A registered plugin with no declared actions must
    // not be able to answer on behalf of the real provider — either to
    // inject falsified data or to steal/grief the response slot before the
    // legitimate provider replies.
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_action_spoof.sock", 19343).await;

    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_action_spoof.sock")
        .await
        .unwrap();
    provider
        .register(
            "weather-provider-real",
            PluginManifest {
                actions: vec!["get_weather".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // An unrelated plugin: registered, but declares no actions at all, so
    // it was never routed anything by the kernel.
    let mut impostor = VynkorClient::connect("/tmp/vynkor_integ_action_spoof.sock")
        .await
        .unwrap();
    impostor
        .register("totally-unrelated-plugin", PluginManifest::default())
        .await
        .unwrap();

    let mut requester = VynkorClient::connect("/tmp/vynkor_integ_action_spoof.sock")
        .await
        .unwrap();
    requester
        .register("weather-requester-2", PluginManifest::default())
        .await
        .unwrap();

    let request_fut = tokio::spawn(async move {
        requester
            .send_action("get_weather", br#"{"city":"nyc"}"#, 2000)
            .await
    });

    // Real provider receives the routed request and learns the internal
    // correlation id — in this test we reuse that same id to stand in for
    // an attacker who has guessed/observed the predictable `kact-<n>` id.
    let received = timeout(Duration::from_secs(2), provider.recv())
        .await
        .expect("provider recv timed out")
        .expect("provider recv failed");
    let internal_action_id = match received.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(req)) => {
            assert_eq!(req.action, "get_weather");
            req.action_id
        }
        other => panic!("expected ActionRequest, got {other:?}"),
    };

    // Impostor races the real provider and sends a spoofed response first.
    let spoofed_env = vynkor::proto::vynkor::Envelope {
        payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(
            vynkor::proto::vynkor::ActionResponse {
                action_id: internal_action_id.clone(),
                status: ActionStatus::ActionOk as i32,
                data_json: br#"{"temp_f":-999,"spoofed":true}"#.to_vec(),
                error: String::new(),
            },
        )),
        ..Default::default()
    };
    impostor.send("kernel", spoofed_env).await.unwrap();

    // Give the kernel a moment to process the spoofed response before the
    // real provider replies, so a bug would actually manifest as the
    // requester observing the spoofed payload.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Real provider now answers truthfully.
    let real_env = vynkor::proto::vynkor::Envelope {
        payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(
            vynkor::proto::vynkor::ActionResponse {
                action_id: internal_action_id,
                status: ActionStatus::ActionOk as i32,
                data_json: br#"{"temp_f":72}"#.to_vec(),
                error: String::new(),
            },
        )),
        ..Default::default()
    };
    provider.send("kernel", real_env).await.unwrap();

    let resp = timeout(Duration::from_secs(2), request_fut)
        .await
        .expect("timed out waiting for requester's response")
        .expect("task panicked")
        .expect("send_action failed");

    // The requester must see the REAL provider's answer, proving the
    // impostor's spoofed response did not consume the pending-action slot.
    assert_eq!(resp.status, ActionStatus::ActionOk as i32);
    assert_eq!(
        resp.data_json, br#"{"temp_f":72}"#,
        "requester received spoofed data instead of the real provider's response"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn ambiguous_action_providers_returns_not_found() {
    // Two plugins both declare the same action name — a deploy
    // misconfiguration, not something the kernel should guess its way
    // through. Must refuse to route, same as zero providers.
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_action_ambiguous.sock", 19344).await;

    let mut provider_a = VynkorClient::connect("/tmp/vynkor_integ_action_ambiguous.sock")
        .await
        .unwrap();
    provider_a
        .register(
            "weather-provider-a",
            PluginManifest {
                actions: vec!["get_weather".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut provider_b = VynkorClient::connect("/tmp/vynkor_integ_action_ambiguous.sock")
        .await
        .unwrap();
    provider_b
        .register(
            "weather-provider-b",
            PluginManifest {
                actions: vec!["get_weather".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut requester = VynkorClient::connect("/tmp/vynkor_integ_action_ambiguous.sock")
        .await
        .unwrap();
    requester
        .register("weather-requester-3", PluginManifest::default())
        .await
        .unwrap();

    let resp = timeout(
        Duration::from_secs(2),
        requester.send_action("get_weather", b"{}", 2000),
    )
    .await
    .expect("timed out")
    .expect("send_action failed");

    assert_eq!(
        resp.status,
        ActionStatus::ActionNotFound as i32,
        "ambiguous provider declaration must not be arbitrarily resolved"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn provider_side_action_failure_proxies_through_unchanged() {
    // A provider that legitimately answers with ACTION_ERROR (or any other
    // non-OK status) must have that status/error relayed to the requester
    // as-is — the kernel is a router here, not a translator.
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_action_failure.sock", 19220).await;

    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_action_failure.sock")
        .await
        .unwrap();
    provider
        .register(
            "flaky-provider",
            PluginManifest {
                actions: vec!["get_weather".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut requester = VynkorClient::connect("/tmp/vynkor_integ_action_failure.sock")
        .await
        .unwrap();
    requester
        .register("weather-requester-4", PluginManifest::default())
        .await
        .unwrap();

    let request_fut =
        tokio::spawn(async move { requester.send_action("get_weather", b"{}", 2000).await });

    let received = timeout(Duration::from_secs(2), provider.recv())
        .await
        .expect("provider recv timed out")
        .expect("provider recv failed");
    let internal_action_id = match received.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(req)) => req.action_id,
        other => panic!("expected ActionRequest, got {other:?}"),
    };

    let err_env = vynkor::proto::vynkor::Envelope {
        payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(
            vynkor::proto::vynkor::ActionResponse {
                action_id: internal_action_id,
                status: ActionStatus::ActionError as i32,
                data_json: vec![],
                error: "upstream weather service unreachable".to_string(),
            },
        )),
        ..Default::default()
    };
    provider.send("kernel", err_env).await.unwrap();

    let resp = timeout(Duration::from_secs(2), request_fut)
        .await
        .expect("timed out")
        .expect("task panicked")
        .expect("send_action failed");

    assert_eq!(resp.status, ActionStatus::ActionError as i32);
    assert_eq!(resp.error, "upstream weather service unreachable");

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn kernel_forwards_request_chunks_to_provider_with_translated_action_id() {
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_stream_upload.sock", 19341).await;

    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_stream_upload.sock")
        .await
        .unwrap();
    provider
        .register(
            "upload-provider",
            PluginManifest {
                actions: vec!["upload".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut requester = VynkorClient::connect("/tmp/vynkor_integ_stream_upload.sock")
        .await
        .unwrap();
    requester
        .register("upload-requester", PluginManifest::default())
        .await
        .unwrap();

    let action_id = requester
        .send_action_streaming("upload", 2000)
        .await
        .unwrap();
    requester
        .send_request_chunk(&action_id, 0, b"hello ".to_vec(), false)
        .await
        .unwrap();
    requester
        .send_request_chunk(&action_id, 1, b"world".to_vec(), true)
        .await
        .unwrap();

    // Provider sees the initial streaming ActionRequest first.
    let received = timeout(Duration::from_secs(2), provider.recv())
        .await
        .expect("provider recv timed out")
        .expect("provider recv failed");
    let internal_action_id = match received.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(req)) => {
            assert_eq!(req.action, "upload");
            assert!(req.streaming);
            req.action_id
        }
        other => panic!("expected streaming ActionRequest, got {other:?}"),
    };

    // Then the two chunks, translated to the internal action_id, in order.
    let chunk0 = timeout(Duration::from_secs(2), provider.recv())
        .await
        .unwrap()
        .unwrap();
    match chunk0.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequestChunk(c)) => {
            assert_eq!(c.action_id, internal_action_id);
            assert_eq!(c.seq, 0);
            assert_eq!(c.chunk, b"hello ");
            assert!(!c.r#final);
        }
        other => panic!("expected ActionRequestChunk, got {other:?}"),
    }

    let chunk1 = timeout(Duration::from_secs(2), provider.recv())
        .await
        .unwrap()
        .unwrap();
    match chunk1.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequestChunk(c)) => {
            assert_eq!(c.action_id, internal_action_id);
            assert_eq!(c.seq, 1);
            assert_eq!(c.chunk, b"world");
            assert!(c.r#final);
        }
        other => panic!("expected final ActionRequestChunk, got {other:?}"),
    }

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn kernel_forwards_response_chunks_to_requester_with_original_action_id() {
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_stream_download.sock", 19302).await;

    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_stream_download.sock")
        .await
        .unwrap();
    provider
        .register(
            "download-provider",
            PluginManifest {
                actions: vec!["download".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut requester = VynkorClient::connect("/tmp/vynkor_integ_stream_download.sock")
        .await
        .unwrap();
    requester
        .register("download-requester", PluginManifest::default())
        .await
        .unwrap();

    let original_action_id = requester
        .send_action_streaming("download", 2000)
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(2), provider.recv())
        .await
        .unwrap()
        .unwrap();
    let internal_action_id = match received.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(req)) => req.action_id,
        other => panic!("expected ActionRequest, got {other:?}"),
    };

    provider
        .send_response_chunk(&internal_action_id, 0, b"chunk-a".to_vec())
        .await
        .unwrap();

    let forwarded = timeout(Duration::from_secs(2), requester.recv())
        .await
        .unwrap()
        .unwrap();
    match forwarded.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionResponseChunk(c)) => {
            assert_eq!(c.action_id, original_action_id);
            assert_eq!(c.seq, 0);
            assert_eq!(c.chunk, b"chunk-a");
        }
        other => panic!("expected ActionResponseChunk, got {other:?}"),
    }

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn stream_backpressure_aborts_both_sides_and_terminates_with_backpressure_status() {
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_stream_backpressure.sock", 19303).await;

    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_stream_backpressure.sock")
        .await
        .unwrap();
    provider
        .register(
            "bp-provider",
            PluginManifest {
                actions: vec!["upload".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut requester = VynkorClient::connect("/tmp/vynkor_integ_stream_backpressure.sock")
        .await
        .unwrap();
    requester
        .register("bp-requester", PluginManifest::default())
        .await
        .unwrap();

    let action_id = requester
        .send_action_streaming("upload", 2000)
        .await
        .unwrap();

    // Drain the provider's registration ack etc. first.
    let _ = timeout(Duration::from_secs(2), provider.recv()).await; // initial ActionRequest

    // Flood far more chunks than the provider's outbound channel capacity
    // without the provider ever draining, forcing a try_send failure on the
    // kernel -> provider hop. (Config's default channel capacity is generous;
    // this loop count matches DEFAULT_CHANNEL_CAPACITY-sized fixtures used
    // elsewhere in this file for the same purpose — see
    // action_concurrency_cap_denies_third_concurrent_call_to_same_provider
    // for the established magnitude convention in this test file.)
    let mut saw_backpressure = false;
    for seq in 0..2000u32 {
        if requester
            .send_request_chunk(&action_id, seq, vec![0u8; 1024], false)
            .await
            .is_err()
        {
            break;
        }
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match timeout(Duration::from_millis(500), requester.recv()).await {
            Ok(Ok(env)) => {
                if let Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(resp)) =
                    env.payload
                {
                    assert_eq!(resp.action_id, action_id);
                    assert_eq!(resp.status, ActionStatus::ActionStreamBackpressure as i32);
                    saw_backpressure = true;
                    break;
                }
            }
            _ => continue,
        }
    }

    assert!(
        saw_backpressure,
        "expected requester to receive an ActionResponse{{status: ACTION_STREAM_BACKPRESSURE}}"
    );

    let _ = shutdown_tx.send(());
}

/// Review-finding regression test (fix pass on Task 4/R6-02): the previous
/// `stream_backpressure_aborts_both_sides...` test only floods the
/// requester -> provider hop (via `send_request_chunk`), which only ever
/// exercises a *provider* channel-full condition. It never exercises the
/// direction that actually triggers the bug: the *requester's* channel full
/// while `abort_stream` is notifying that very same requester.
///
/// `abort_stream` previously used blocking `send_envelope(...).await` for
/// its two requester-facing notifications (`ActionStreamAbort` and the
/// terminal `ActionResponse`). When invoked from the `ActionResponseChunk`
/// arm because forwarding a response chunk to the requester just failed
/// (full-but-alive channel), those two blocking sends targeted that same
/// full channel and would stall the entire shared router loop
/// (`run_with_context` processes every connection's messages sequentially)
/// until the requester drained — i.e. the exact scenario non-blocking
/// backpressure handling exists to prevent.
///
/// This test floods in the provider -> requester direction (via
/// `send_response_chunk`) to fill the *requester's* inbound channel, and
/// proves the router keeps servicing other connections promptly by having
/// an unrelated third client ping the kernel while the flood is in flight —
/// mirroring how `tests/unit/test_router.rs`'s
/// `forward_to_full_channel_returns_without_waiting` proves non-blocking
/// behavior via timing rather than just asserting eventual delivery.
#[tokio::test]
async fn stream_backpressure_on_requester_channel_does_not_stall_router() {
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_stream_bp_requester.sock", 19304).await;

    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_stream_bp_requester.sock")
        .await
        .unwrap();
    provider
        .register(
            "bp-req-provider",
            PluginManifest {
                actions: vec!["download".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Deliberately never drained: this is the channel we want to fill.
    let mut requester = VynkorClient::connect("/tmp/vynkor_integ_stream_bp_requester.sock")
        .await
        .unwrap();
    requester
        .register("bp-req-requester", PluginManifest::default())
        .await
        .unwrap();

    requester
        .send_action_streaming("download", 2000)
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(2), provider.recv())
        .await
        .unwrap()
        .unwrap();
    let internal_action_id = match received.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(req)) => req.action_id,
        other => panic!("expected ActionRequest, got {other:?}"),
    };

    // A third, unrelated connection: proves the shared router loop keeps
    // servicing other connections promptly while the flood below is
    // in flight and the requester's channel is full.
    let mut bystander = VynkorClient::connect("/tmp/vynkor_integ_stream_bp_requester.sock")
        .await
        .unwrap();
    bystander
        .register("bp-req-bystander", PluginManifest::default())
        .await
        .unwrap();

    // Flood response chunks from the provider without the requester ever
    // draining, forcing a try_send failure on the kernel -> requester hop
    // (same magnitude convention as the existing backpressure test).
    for seq in 0..2000u32 {
        if provider
            .send_response_chunk(&internal_action_id, seq, vec![0u8; 1024])
            .await
            .is_err()
        {
            break;
        }
    }

    // While the requester's channel is (very likely) full and abort_stream
    // may be firing repeatedly against it, the router must still service
    // the bystander's ping promptly. Before the fix, abort_stream's
    // blocking sends to the stuck requester channel would stall the shared
    // router loop for every other connection until the requester drained
    // (which never happens here) — this ping would then time out.
    let pong = timeout(Duration::from_millis(500), bystander.ping()).await;
    assert!(
        pong.is_ok(),
        "router appears stalled: bystander ping did not complete promptly \
         while requester channel was under backpressure (abort_stream may \
         be blocking on the full requester channel)"
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn session_streaming_accept_exchange_and_graceful_close() {
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_session_lifecycle.sock", 19310).await;

    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_session_lifecycle.sock")
        .await
        .unwrap();
    provider
        .register(
            "session-provider",
            PluginManifest {
                actions: vec!["ws_session".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut requester = VynkorClient::connect("/tmp/vynkor_integ_session_lifecycle.sock")
        .await
        .unwrap();
    requester
        .register("session-requester", PluginManifest::default())
        .await
        .unwrap();

    let action_id = requester
        .send_action_streaming("ws_session", 2000)
        .await
        .unwrap();

    // Provider sees the initial streaming ActionRequest, accepts it.
    let received = timeout(Duration::from_secs(2), provider.recv())
        .await
        .unwrap()
        .unwrap();
    let internal_action_id = match received.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(req)) => {
            assert!(req.streaming);
            req.action_id
        }
        other => panic!("expected streaming ActionRequest, got {other:?}"),
    };

    provider
        .send(
            "kernel",
            vynkor::proto::vynkor::Envelope {
                payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(
                    vynkor::proto::vynkor::ActionResponse {
                        action_id: internal_action_id.clone(),
                        status: ActionStatus::ActionOk as i32,
                        data_json: vec![],
                        error: String::new(),
                    },
                )),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Requester gets the accepting ActionResponse.
    let accept = timeout(Duration::from_secs(2), requester.recv())
        .await
        .unwrap()
        .unwrap();
    match accept.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(resp)) => {
            assert_eq!(resp.action_id, action_id);
            assert_eq!(resp.status, ActionStatus::ActionOk as i32);
        }
        other => panic!("expected accepting ActionResponse, got {other:?}"),
    }

    // Multiple round trips of chunks, both directions, after acceptance.
    requester
        .send_request_chunk(&action_id, 0, b"ping-1".to_vec(), false)
        .await
        .unwrap();
    let c = timeout(Duration::from_secs(2), provider.recv())
        .await
        .unwrap()
        .unwrap();
    match c.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequestChunk(c)) => {
            assert_eq!(c.action_id, internal_action_id);
            assert_eq!(c.chunk, b"ping-1");
        }
        other => panic!("expected ActionRequestChunk, got {other:?}"),
    }

    provider
        .send_response_chunk(&internal_action_id, 0, b"pong-1".to_vec())
        .await
        .unwrap();
    let c = timeout(Duration::from_secs(2), requester.recv())
        .await
        .unwrap()
        .unwrap();
    match c.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionResponseChunk(c)) => {
            assert_eq!(c.action_id, action_id);
            assert_eq!(c.chunk, b"pong-1");
        }
        other => panic!("expected ActionResponseChunk, got {other:?}"),
    }

    requester
        .send_request_chunk(&action_id, 1, b"ping-2".to_vec(), false)
        .await
        .unwrap();
    let c = timeout(Duration::from_secs(2), provider.recv())
        .await
        .unwrap()
        .unwrap();
    match c.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequestChunk(c)) => {
            assert_eq!(c.chunk, b"ping-2");
        }
        other => panic!("expected ActionRequestChunk, got {other:?}"),
    }

    // Graceful close from the requester forwards to the provider.
    requester.close_session(&action_id, "done").await.unwrap();

    let closed = timeout(Duration::from_secs(2), provider.recv())
        .await
        .unwrap()
        .unwrap();
    match closed.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::SessionClose(close)) => {
            assert_eq!(close.action_id, internal_action_id);
            assert_eq!(close.reason, "done");
        }
        other => panic!("expected SessionClose, got {other:?}"),
    }

    // Session is evicted: a second close attempt is rejected (no matching session).
    let close_again = requester.close_session(&action_id, "done again").await;
    assert!(
        close_again.is_ok(),
        "sending must succeed even though the kernel will reject it"
    );
    let err = timeout(Duration::from_secs(2), requester.recv())
        .await
        .unwrap()
        .unwrap();
    match err.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::Error(e)) => {
            assert!(!e.message.is_empty());
        }
        other => panic!("expected an Error for closing an already-evicted session, got {other:?}"),
    }

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn session_streaming_rejection_evicts_without_session_close() {
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_session_reject.sock", 19311).await;

    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_session_reject.sock")
        .await
        .unwrap();
    provider
        .register(
            "session-reject-provider",
            PluginManifest {
                actions: vec!["ws_session".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut requester = VynkorClient::connect("/tmp/vynkor_integ_session_reject.sock")
        .await
        .unwrap();
    requester
        .register("session-reject-requester", PluginManifest::default())
        .await
        .unwrap();

    let action_id = requester
        .send_action_streaming("ws_session", 2000)
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(2), provider.recv())
        .await
        .unwrap()
        .unwrap();
    let internal_action_id = match received.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(req)) => req.action_id,
        other => panic!("expected ActionRequest, got {other:?}"),
    };

    provider
        .send(
            "kernel",
            vynkor::proto::vynkor::Envelope {
                payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(
                    vynkor::proto::vynkor::ActionResponse {
                        action_id: internal_action_id,
                        status: ActionStatus::ActionError as i32,
                        data_json: vec![],
                        error: "provider refused the session".to_string(),
                    },
                )),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let rejected = timeout(Duration::from_secs(2), requester.recv())
        .await
        .unwrap()
        .unwrap();
    match rejected.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(resp)) => {
            assert_eq!(resp.action_id, action_id);
            assert_eq!(resp.status, ActionStatus::ActionError as i32);
        }
        other => panic!("expected error ActionResponse, got {other:?}"),
    }

    // No SessionClose is needed or sent — a rejection evicts immediately,
    // same as any non-streaming failure. Confirm eviction: close_session
    // now gets rejected as "no matching session".
    requester
        .close_session(&action_id, "irrelevant")
        .await
        .unwrap();
    let err = timeout(Duration::from_secs(2), requester.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        err.payload,
        Some(vynkor::proto::vynkor::envelope::Payload::Error(_))
    ));

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn session_close_before_acceptance_is_rejected() {
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_session_close_early.sock", 19312).await;

    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_session_close_early.sock")
        .await
        .unwrap();
    provider
        .register(
            "session-early-provider",
            PluginManifest {
                actions: vec!["ws_session".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut requester = VynkorClient::connect("/tmp/vynkor_integ_session_close_early.sock")
        .await
        .unwrap();
    requester
        .register("session-early-requester", PluginManifest::default())
        .await
        .unwrap();

    let action_id = requester
        .send_action_streaming("ws_session", 5000)
        .await
        .unwrap();

    // Provider never responds. Requester tries to close before any accept.
    let _ = timeout(Duration::from_secs(2), provider.recv()).await; // drain the initial ActionRequest

    requester
        .close_session(&action_id, "changed my mind")
        .await
        .unwrap();

    let err = timeout(Duration::from_secs(2), requester.recv())
        .await
        .unwrap()
        .unwrap();
    match err.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::Error(e)) => {
            assert!(!e.message.is_empty());
        }
        other => panic!("expected an Error for closing before acceptance, got {other:?}"),
    }

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn session_close_from_third_party_is_rejected() {
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_session_close_thirdparty.sock", 19313).await;

    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_session_close_thirdparty.sock")
        .await
        .unwrap();
    provider
        .register(
            "session-tp-provider",
            PluginManifest {
                actions: vec!["ws_session".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut requester = VynkorClient::connect("/tmp/vynkor_integ_session_close_thirdparty.sock")
        .await
        .unwrap();
    requester
        .register("session-tp-requester", PluginManifest::default())
        .await
        .unwrap();

    let mut impostor = VynkorClient::connect("/tmp/vynkor_integ_session_close_thirdparty.sock")
        .await
        .unwrap();
    impostor
        .register("session-tp-impostor", PluginManifest::default())
        .await
        .unwrap();

    let action_id = requester
        .send_action_streaming("ws_session", 2000)
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(2), provider.recv())
        .await
        .unwrap()
        .unwrap();
    let internal_action_id = match received.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(req)) => req.action_id,
        other => panic!("expected ActionRequest, got {other:?}"),
    };

    provider
        .send(
            "kernel",
            vynkor::proto::vynkor::Envelope {
                payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(
                    vynkor::proto::vynkor::ActionResponse {
                        action_id: internal_action_id.clone(),
                        status: ActionStatus::ActionOk as i32,
                        data_json: vec![],
                        error: String::new(),
                    },
                )),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let _ = timeout(Duration::from_secs(2), requester.recv()).await; // drain the accept

    // Impostor guesses the internal action_id and tries to close it.
    impostor
        .close_session(&internal_action_id, "not yours")
        .await
        .unwrap();
    let err = timeout(Duration::from_secs(2), impostor.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        err.payload,
        Some(vynkor::proto::vynkor::envelope::Payload::Error(_))
    ));

    // Session must still be open for the real requester.
    requester
        .close_session(&action_id, "the real close")
        .await
        .unwrap();
    let closed = timeout(Duration::from_secs(2), provider.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        closed.payload,
        Some(vynkor::proto::vynkor::envelope::Payload::SessionClose(_))
    ));

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn session_idle_timeout_aborts_both_sides() {
    let mut cfg = test_config("/tmp/vynkor_integ_session_idle.sock", 19314);
    cfg.session_idle_timeout_secs = Some(1);
    cfg.prune_interval_secs = 2;
    let (shutdown_tx, _registry, _bus) = start_kernel_with_config(cfg).await;

    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_session_idle.sock")
        .await
        .unwrap();
    provider
        .register(
            "session-idle-provider",
            PluginManifest {
                actions: vec!["ws_session".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut requester = VynkorClient::connect("/tmp/vynkor_integ_session_idle.sock")
        .await
        .unwrap();
    requester
        .register("session-idle-requester", PluginManifest::default())
        .await
        .unwrap();

    let action_id = requester
        .send_action_streaming("ws_session", 5000)
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(2), provider.recv())
        .await
        .unwrap()
        .unwrap();
    let internal_action_id = match received.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(req)) => req.action_id,
        other => panic!("expected ActionRequest, got {other:?}"),
    };

    provider
        .send(
            "kernel",
            vynkor::proto::vynkor::Envelope {
                payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(
                    vynkor::proto::vynkor::ActionResponse {
                        action_id: internal_action_id,
                        status: ActionStatus::ActionOk as i32,
                        data_json: vec![],
                        error: String::new(),
                    },
                )),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let _ = timeout(Duration::from_secs(2), requester.recv()).await; // drain the accept

    // Do nothing for longer than session_idle_timeout_secs (1s) + one prune
    // tick (2s, set via cfg.prune_interval_secs) — the idle sweep only runs
    // on a prune tick, so wait for the tick to fire.
    let abort = timeout(Duration::from_secs(10), requester.recv())
        .await
        .expect("must receive an abort within one prune tick after idling")
        .unwrap();
    match abort.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionStreamAbort(a)) => {
            assert_eq!(a.action_id, action_id);
            assert_eq!(a.reason, "idle timeout");
        }
        other => panic!("expected ActionStreamAbort, got {other:?}"),
    }

    let provider_abort = timeout(Duration::from_secs(5), provider.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        provider_abort.payload,
        Some(vynkor::proto::vynkor::envelope::Payload::ActionStreamAbort(
            _
        ))
    ));

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn session_idle_timeout_unset_leaves_accepted_session_open() {
    let (shutdown_tx, _registry, _bus) =
        start_kernel("/tmp/vynkor_integ_session_idle_unset.sock", 19315).await;

    let mut provider = VynkorClient::connect("/tmp/vynkor_integ_session_idle_unset.sock")
        .await
        .unwrap();
    provider
        .register(
            "session-idle-unset-provider",
            PluginManifest {
                actions: vec!["ws_session".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut requester = VynkorClient::connect("/tmp/vynkor_integ_session_idle_unset.sock")
        .await
        .unwrap();
    requester
        .register("session-idle-unset-requester", PluginManifest::default())
        .await
        .unwrap();

    let action_id = requester
        .send_action_streaming("ws_session", 5000)
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(2), provider.recv())
        .await
        .unwrap()
        .unwrap();
    let internal_action_id = match received.payload {
        Some(vynkor::proto::vynkor::envelope::Payload::ActionRequest(req)) => req.action_id,
        other => panic!("expected ActionRequest, got {other:?}"),
    };

    provider
        .send(
            "kernel",
            vynkor::proto::vynkor::Envelope {
                payload: Some(vynkor::proto::vynkor::envelope::Payload::ActionResponse(
                    vynkor::proto::vynkor::ActionResponse {
                        action_id: internal_action_id,
                        status: ActionStatus::ActionOk as i32,
                        data_json: vec![],
                        error: String::new(),
                    },
                )),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let _ = timeout(Duration::from_secs(2), requester.recv()).await; // drain the accept

    // With session_idle_timeout_secs unset (test_config's default), no abort
    // should arrive even after the idle sweep would otherwise have fired.
    let result = timeout(Duration::from_secs(3), requester.recv()).await;
    assert!(
        result.is_err(),
        "no message expected: an unset idle timeout must leave the session open indefinitely"
    );

    // Session is still usable: a graceful close still works.
    requester.close_session(&action_id, "done").await.unwrap();
    let closed = timeout(Duration::from_secs(2), provider.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        closed.payload,
        Some(vynkor::proto::vynkor::envelope::Payload::SessionClose(_))
    ));

    let _ = shutdown_tx.send(());
}
