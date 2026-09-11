# Vynkor ROADMAP — Phases 8–11

**Baseline:** 2026-08-10 · Kernel `0.1.0`
**Branch:** `develop`
**Previous phases:** `docs/archive/` (Phase 1–2: `ROADMAP_phase1.md`/`ROADMAP_v2.md`/`ROADMAP_v3.md` · Phase 3–4: `ROADMAP_v4.md` · Phase 5: `ROADMAP_v5.md` · Phase 6: `ROADMAP_v6.md` · Phase 7 (C++/Python SDK parity): `ROADMAP_v7.md`, all items complete)

---

## Manifesto (non-negotiable)

- Kernel = dumb byte router + process supervisor. Zero business logic. Zero AI. Zero databases for application state (the event-delivery outbox is the explicit exception, see DC-5/F6).
- Intra-host IPC = UDS only. No TCP, no Redis, no queues.
- Protocol = single `.proto` file. Changes propagate to all SDKs.
- Plugin = isolated OS process. Cannot bypass kernel. Speaks only UDS.
- External access = WebSocket/HTTP gateway only (Axum).

---

## Current baseline — 2026-08-10

Phase 7 shipped full C++/Python SDK parity with the Rust reference client
(`publish_event`, streaming actions, session close, cross-SDK integration
tests). On the protocol side, kernel support for `PERMISSION_STORAGE` and
`ActionRequest.caller_plugin_id` landed on `develop` (`13274d4`,
`e00ec96`, `6b3691f`) — but the marketplace installer's permission allowlist
(`src/marketplace/installer.rs`) was never updated to match, so installing the
`database` plugin fails with `Plugin 'database' declares unknown permission
'PERMISSION_STORAGE'`. Phase 8 fixes that drift at the root: the known
permission set stops being a hand-maintained list and is derived from the
`PermissionType` proto enum itself, so it can never fall behind the protocol
again.

## Phase 8 — Permission/protocol sync

The proto's `PermissionType` enum is the single source of truth for what a
plugin may declare. Phase 7's protocol work proved the enum can grow
(`PERMISSION_EVENT_PUBLISH`, `PERMISSION_STORAGE`) while the installer's
hand-typed list quietly rots. This phase derives the validation set from the
generated enum, adds drift-detection tests, and lands the follow-ups the
`database` plugin needs across the sibling repos.

- [x] R8-01 — **Installer permission validation derived from the proto enum:**
  replace `const KNOWN_PERMISSIONS` in `src/marketplace/installer.rs` with a
  set derived from `PermissionType::values()` (prost `Enumeration`), accepting
  both the documented lowercase form (`storage`) and the `PERMISSION_`-prefixed
  proto name (`PERMISSION_STORAGE`); unknown permissions keep the exact
  existing error.
  - Files: `src/marketplace/installer.rs`, `tests/unit/test_installer.rs`.
  - Acceptance: `vyn plugin install database` no longer fails with the
    unknown-permission error; `"teleport"` is still rejected.

- [x] R8-02 — **Permission drift-detection tests:** new test walking
  `PermissionType::values()`: every variant except `PERMISSION_UNKNOWN` must
  pass `validate_manifest` in both forms — a future proto permission can never
  silently fail installation again.
  - Files: `tests/unit/test_installer.rs`.
  - Acceptance: `cargo test --test unit manifest_accepts_every_proto_permission` green.

- [x] R8-03 — **Runtime `check_permission` form normalization:**
  `src/auth/permissions.rs::check_permission` compares declared permissions
  against `as_str_name()` with exact equality; normalize the comparison
  (case-insensitive, `PERMISSION_`-prefix-stripped on both sides) so a plugin
  declaring the lowercase form is not denied at runtime.
  - Files: `src/auth/permissions.rs`, unit tests.
  - Acceptance: a manifest declaring `"network"` satisfies a
    `PermissionNetwork` requirement; missing permissions still denied.

- [x] R8-04 — **Registry schema doc alignment:** `docs/PLUGIN_REGISTRY_SCHEMA.md`
  adds `storage`/`event_publish` rows to the permission mapping table and
  notes that the allowed set is derived from the `PermissionType` proto enum
  (both forms accepted).
  - Acceptance: mapping rows present; no schema restructuring.

- [x] R8-05 — **Proto-copy byte-identity drift test:** test asserting the three
  vendored `vynkor_protocol.proto` copies (`wire/proto`, `sdk/python/proto`,
  `sdk/cpp/proto`) stay byte-identical.
  - Files: `tests/unit/test_proto_sync.rs` (new), `tests/unit/mod.rs`.
  - Acceptance: test passes; editing one copy makes it fail.

- [x] R8-06 — **(cross-repo) Land the `database` plugin in vynkor-plugins:**
  merge `worktree-database-plugin` into `vynkor-plugins` `main`
  (`plugins/database/`), add the registry entry via `scripts/package.sh`
  (permissions normalized to `["storage"]`), build `dist/database-0.1.0.zip`,
  move `database` from Planned to Shipped in `vynkor-plugins/ROADMAP.md`.
  - Tracked in: `vynkor-plugins/ROADMAP.md`.
  - Acceptance: `vyn plugin install database` against the fixed kernel succeeds
    and the plugin registers.

- [x] R8-07 — **(cross-repo) Publish `vynkor-wire` 0.2.0, drop patch override:**
  `cargo publish` `vynkor-wire` 0.2.0 (kernel already depends on it by
  path + `[patch.crates-io]` override whose comment says it is a no-op once
  0.2.0 is published); remove the override and verify the workspace still
  builds green.
- Tracked in: `vynkor-wire/`.
- Acceptance: `cargo search vynkor-wire` shows 0.2.0; no `patch.crates-io`
  block in `Cargo.toml`; full test suite green without it.

---

## Immediate — Audit findings (nearest)

Findings from the 2026-08-11 audit reconciliation (see `AUDIT.md`). All are
kernel-local, independent of the cross-repo R8 items, and land before any
Phase 9 work. N5 restores the DoD gates; N2 closes the last
permission-surface inconsistency; N1 is the largest single hot-path win.

> **STATUS (2026-08-11): N1–N5 all shipped on `develop`.** N1 closed as
> non-issue — wire v0.2.0 already shares the payload via `Arc<[u8]>`; sharing
> locked in by regression tests. N2–N5 fixed with regression coverage (see
> each item). N5 restores the DoD `fmt` gate.

- [x] N1 — **Router hot path clones every forwarded payload (Moderate):**
  `forward` (`protocol.rs:995`) does `payload: msg.frame.payload.clone()`
  — a full `Vec<u8>` heap copy per frame (up to 1 MiB); `broadcast` has the
  same pattern. The event path already shares bytes via `Arc<[u8]>`
  (`events/bus.rs:121`). Contradicts the README "zero copies" claim and is
  the dominant router cost at high throughput.
  - Files: `src/ipc/protocol.rs`.
  - Acceptance: `forward`/`broadcast` route a payload-sharing frame
    (`Arc<[u8]>`) without per-hop clone, mirroring `EventBus::deliver`;
    README §3 updated only if the sharing semantics change.
  - **Status (2026-08-11): CLOSED — non-issue.** `Frame.payload` is already
    `Arc<[u8]>` in wire v0.2.0 (`wire/src/framing.rs:69-81`), so the clone is
    a refcount bump. Regression tests `forward_shares_payload_without_copy`
    / `broadcast_shares_payload_without_copy` (`tests/unit/test_router.rs`)
    assert `Arc::ptr_eq` on a 64 KiB payload.

- [x] N2 — **Permission comparison is form-sensitive in the clamp + config
      cross-check (Low, fails-closed):** the T-04 registration clamp
      (`protocol.rs:336`) and `validate_plugin_def` (`loader.rs:249`)
      compare permission strings with exact equality, while runtime
      `check_permission` normalizes (`permissions.rs:21-25,36`). A config
      `permissions: [network]` with a token claiming `PERMISSION_NETWORK`
      silently strips the permission at registration (warn only) or
      refuses boot.
  - Files: `src/ipc/protocol.rs`, `src/plugins/loader.rs`, `src/auth/permissions.rs`.
  - Acceptance: normalize both sides; extend
    `registration_clamps_jwt_permissions_to_config_allowlist`
    (`tests/unit/test_router.rs:948`) to cover both forms.
  - **Status (2026-08-11): FIXED** — `normalize_permission` is `pub(crate)`;
    the clamp builds a normalized `HashSet` (`protocol.rs:336-343`);
    `validate_plugin_def` uses a normalized `any` match (`loader.rs:250-260`).
    Covered by the parametrized clamp test (both forms) and
    `config_lowercase_perm_matches_manifest_proto_form` (with negative control).

- [x] N3 — **`load_config` performs no numeric bounds validation (Low):**
  `config.rs:318` accepts `router_channel_capacity: 0`, `max_connections: 0`,
  and negative watchdogs silently.
  - Files: `src/utils/config.rs`.
  - Acceptance: out-of-range numerics clamp to defaults or error loudly.
  - **Status (2026-08-11): FIXED** — `clamp_invalid_numerics` clamps the four
    zero-invalid fields to defaults with `warn!` (fields are unsigned, so
    negatives already fail serde). Covered by
    `load_config_clamps_zero_numerics_to_defaults` /
    `load_config_preserves_sane_numerics`.

- [x] N4 — **Daemon start reports success before the child holds the
      pid-file lock (Low, TOCTOU):** `daemonize_and_run` (`main.rs:213-236`)
      writes the pid and returns success; the re-exec'd child acquires the
      exclusive flock only later in `run_foreground` (`main.rs:238-264`) —
      a competing instance winning the lock first aborts the child after
      the parent already reported "started".
  - Files: `src/main.rs`.
  - Acceptance: readiness handshake — the child reports success (exit
    status or explicit ready line) before the parent confirms.
  - **Status (2026-08-11): FIXED** — `UnixStream::pair()` handshake via
    `VYNKOR_READY_FD` (CLOEXEC cleared in `pre_exec`): the child emits
    `"{pid}\n"` only after flock + pid write; the parent publishes the pid
    file and reports success only after the matching line, else SIGKILLs +
    reaps + cleans up and errors out. Smoke-verified happy and failure paths.

- [x] N5 — **`cargo fmt --check` fails on `tests/unit/test_proto_sync.rs:43`
      (Low, DoD violation):** unformatted closure (introduced `61aec96`);
      the only offender against the Definition of Done gate.
  - Files: `tests/unit/test_proto_sync.rs`.
  - Acceptance: `cargo fmt --check` exits 0.
  - **Status (2026-08-11): FIXED** — `cargo fmt` run tree-wide; `fmt --check` exits 0.

> Deferred audit items M7 (C++/Python fuzz harness) remains open — tracked in
> the Task Summary below and in `AUDIT.md`. M9 (zero-value enum renumber)
> shipped with the v1.5 bump (P11-03, 2026-08-13). M7 is the last substantive
> coverage gap.

---

## Immediate — Verification bugs: R9-02 supervisor stop/start race (found 2026-08-12)

Found live-testing the R9-02 shim on `develop` (`testing` ≡ `develop` @
`8c6a86b`): a stop/start race in the supervisor rework. `stop` returns
success before the shim actually exits (the grace escalation delays the real
exit by up to 5s), and `ExitEvent` carries no PID/epoch — so a stale exit of
the old instance lands on the *new* entry and triggers an unwanted restart →
duplicate registration.

- [x] B1 — **Stop/start race: stale `ExitEvent` (no PID/epoch) restarts the
      wrong instance → duplicate registration:** `stop_plugin` removes the
      entry and SIGTERMs the shim but does not wait for the exit; the shim's
      grace escalation delays the real exit by up to 5s. `ExitEvent` carries
      only `{plugin_id, success}` — a `start` inside that window attributes
      the old instance's exit to the new entry → `OnFailure` → auto-restart →
      duplicate spawn → `registration rejected: plugin already registered`
      (observed twice in the kernel log: pids 112747, 114277).
  - Files: `src/plugins/supervisor.rs`.
  - Acceptance: `stop` + immediate `start` never produces a duplicate
    registration; a stale exit of the old instance is ignored.
  - **Status (2026-08-12): FIXED** — `ExitEvent` now carries the spawn's
    `epoch` (plus `pid`); `monitor_loop` drops any exit whose epoch ≠ the
    registered entry's (stale) or that matches `stopped_epochs` (explicitly
    stopped instance).

- [x] B2 — **`stop` swallows ESRCH and can orphan the live instance:**
      `let _ = kill(pid, SIGTERM)` targets the entry pid, which may already
      be dead, while the actually-registered instance keeps running —
      observed unsupervised for minutes after `stop` reported "stopped"
      (pids 112505/112506).
  - Files: `src/plugins/supervisor.rs`.
  - Acceptance: `stop` always terminates the live registered instance and
    waits for it to exit; ESRCH is handled explicitly, not swallowed.
  - **Status (2026-08-12): FIXED** — `stop_plugin` records the stopped
    epoch, SIGTERMs `signal_target()` (the shim when sandboxed — always the
    live instance), then blocks on `wait_for_exit` (watch channel fired by
    the wait task after reap) with a SIGKILL deadline of `grace_seconds` and
    a final 10s bound — "stopped" now means the tree actually exited.

- [x] B3 — **`spawn_internal` overwrites the manual-start entry on a
      rejected duplicate restart:** the auto-restart path's `entries.insert`
      replaces the operator's freshly-started entry with the duplicate's.
  - Files: `src/plugins/supervisor.rs`.
  - Acceptance: a duplicate-registration restart never clobbers the
    currently-registered entry.
  - **Status (2026-08-12): FIXED** — `spawn_internal` takes
    `replace_epoch: Option<u64>`; manual start (`None`) refuses with
    `VynkorError::PluginAlreadyRunning` while an entry is registered;
    supervised restarts pass `Some(event.epoch)` and only replace the
    instance they were decided for. A stop landing during the backoff
    window (`stopped_during_backoff`) cancels the restart outright.

- [x] B4 — **cgroup scope reap loops on `Device or resource busy`:** when a
      new instance joins the same `vynkor/<id>.scope` before the old one
      exits, the rmdir keeps failing EBUSY and is retried indefinitely.
  - Files: `src/plugins/supervisor.rs`.
  - Acceptance: the old scope is always reaped once its last task exits; no
    unbounded retry loop in the log.
  - **Status (2026-08-12): FIXED** — the wait task fires the `exited`
    signal only after the reap (bounded 10×50ms retry) completes, and
    `stop_plugin` blocks on that signal — a `start` reusing the scope can no
    longer interleave with the old instance's rmdir.

> Environment artifacts (not kernel bugs, 2026-08-12): installed plugins in
> `~/.local/lib/vyn/plugins/` are pre-wire-0.2.0 builds — their registered
> manifest has empty `actions` (→ `ActionNotFound`) and the watchdog SIGKILLs
> them as unresponsive (Ping proto drift) until `max_restarts` is exhausted.
> Reinstall/rebuild them. Also: the local `vynkor-sdk` (0.1.0) requires
> `vynkor-wire ^0.1.0`, which is yanked on crates.io, so path consumers cannot
> resolve; the kernel itself uses the published `vynkor-sdk 0.1.2` (wire
> 0.2.0). Recorded here so the next session doesn't re-derive it.
>
> **RESOLVED (2026-08-12):** `vynkor-sdk-rust` bumped to 0.1.2 + wire 0.2.0
> (commit `0742bd2`); `ping-pong-rs` migrated off the stale git deps /
> `vynkor::` monolith imports to crates.io `vynkor-sdk 0.1`; `ping-pong-rs`,
> `network`, `ai` rebuilt against wire 0.2.0 and reinstalled into
> `~/.local/lib/vyn/plugins/`. Live check: all three register with populated
> `actions`, `ping → pong` returns `ACTION_OK`, zero watchdog SIGKILLs,
> `restart_count=0`. (Vynkor-plugins repo: `plugins/ping-pong-rs/{Cargo.toml,
> src/main.rs}` fixed — commit pending there.)

---

## Immediate — Delta audit findings (2026-08-14)

Fresh full-repo audit (see `AUDIT.md` → "Delta audit — 2026-08-14") on
`develop` @ `2d16ebf` — everything added since the 2026-08-11 reconciliation
(R10-02/R10-03/R10-04, manifest v2, Landlock/seccomp/shim) plus the
previously un-audited performance and UX surfaces. Method: three parallel
read-only passes + `cargo audit` + targeted verification. All items below are
**OPEN** (2026-08-14).

Priorities: **P0** do first (trust-anchor correctness) · **P1** immediate
(security + hot-path perf) · **P2** this cycle (UX + moderate perf) · **P3**
backlog (polish).

### P0 — trust anchor

- [x] S1 — **Registry entry signature does not bind `status`/`archive_url` —
      revocation bypass + download redirect (Medium):**
      `RegistryEntry.signature` covers only `"{slug}:{version}:{sha256}"`
      (`registry.rs:65-70`); `status`, `archive_url`, `min/max_kernel_version`
      and `permissions` are unsigned. A compromised registry channel (the
      exact threat model the signature was added for, M4/T-11) can flip
      `revoked → stable` — the entry still verifies, the `is_revoked` gate
      (`installer.rs:181`) passes, and a revoked plugin installs. The same
      channel can redirect `archive_url` to an arbitrary URL (request
      forgery against internal services; content integrity survives via the
      signed sha256) and loosen kernel-compat bounds.
  - Files: `src/marketplace/registry.rs`, `src/marketplace/installer.rs`.
  - Acceptance: `verify_entry_signature` binds the full canonical entry (at
    minimum `slug:version:sha256:status:archive_url:min/max_kernel_version`);
    flipping `status` or `archive_url` on a signed entry fails verification.
  - **Status (2026-08-14): FIXED** — `signed_message` now covers the full
    canonical entry `slug:version:sha256:status:archive_url:min/max_kernel_version`
    (as served — a relative `archive_url` verifies in its raw form). Relative-URL
    resolution moved out of `fetch_registry_from` into `install()`, after
    verification and before the download, so the signature check runs before any
    request to `archive_url` (a forged URL is never fetched). Cache schema bumped
    to v2 (v1 caches hold resolved URLs + old-format signatures). Regression
    tests: `signature_rejected_when_{status,archive_url,kernel_bounds}_tampered`,
    `relative_archive_url_verifies_in_as_served_form`,
    `fetch_keeps_relative_archive_url_as_served`,
    `install_rejects_*_before_download` (install-level).

### P1 — immediate

- [x] S3 — **RUSTSEC-2026-0204 `crossbeam-epoch` 0.9.18 (Low, one-command
      fix):** invalid pointer dereference in the `fmt::Pointer` impl; reached
      via `metrics-exporter-prometheus 0.15.3 → metrics-util 0.17.0`.
  - Files: `Cargo.lock`.
  - Acceptance: `cargo update -p crossbeam-epoch` → 0.9.20; `cargo audit`
    reports zero vulnerabilities.
  - **Status (2026-08-14): FIXED** — `cargo update -p crossbeam-epoch` →
    0.9.20 (PR #20, `0f5d65f`); `cargo audit` no longer reports
    RUSTSEC-2026-0204 (only the two S4 warnings remain).

- [x] S2 — **`data_dir: /tmp/vyn` puts the events SQLite DB in
      world-writable /tmp (Low-Med):** `EventStore::new` (`events/store.rs:13-16`)
      does `create_dir_all` + symlink-following `Connection::open`. On a
      multi-user host a local user can pre-create `/tmp/vyn` and read or
      forge the event store — fake pending events get redelivered to
      subscribers by the retry worker. Contradicts the config file's own
      M-09 claim ("never the shared /tmp").
  - Files: `src/events/store.rs`, `src/utils/config.rs`, `config.yaml`.
  - Acceptance: `data_dir` defaults to the per-user private runtime dir; the
    store dir is created 0o700 with an ownership check.
  - **Status (2026-08-18): FIXED** — `default_data_dir()` uses
    `vynkor_wire::socket::default_private_dir()` (XDG_RUNTIME_DIR pattern);
    `EventStore::new` rejects world-writable dirs; shipped via PR #35.

- [x] PERF-1 — **Router kernel replies block on `.send().await` — one slow
      plugin stalls all IPC (Medium):** `send_envelope`
      (`protocol.rs:1145-1149`) awaits the target's 64-slot write channel
      from the single shared router task; peer forwards already use
      `try_send` (`protocol.rs:1011,1085`), kernel replies (Pong, acks,
      errors) do not.
  - Files: `src/ipc/protocol.rs`.
  - Acceptance: a plugin that stops draining its write channel delays only
    its own kernel replies; other connections are unaffected.
  - **Status (2026-08-26): FIXED** — `send_envelope`/`send_error`/
    `send_register_reject` are now sync + `try_send`: a full channel drops
    the reply (`kernel_replies_dropped_total` counter + `warn!`), a closed
    one logs `debug`. Regression test
    `kernel_replies_do_not_block_router_on_full_peer_channel`
    (tests/unit/test_router.rs) saturates a peer's channel and asserts the
    router stays live for other connections (hangs on the old code).

- [x] PERF-2 — **Synchronous SQLite + std Mutex on the async runtime in the
      router path (Medium):** `EventBus::publish → store.persist`
      (`bus.rs:85`), `mark_delivered` (`protocol.rs:929`) and the retry
      worker (`bus.rs:160-180`) run blocking rusqlite under
      `std::sync::Mutex<Connection>` (`events/store.rs:9,38`) on tokio
      workers — disk I/O on the hottest path.
  - Files: `src/events/store.rs`, `src/events/bus.rs`, `src/ipc/protocol.rs`.
  - Acceptance: event persistence never blocks the router task
    (`tokio::task::spawn_blocking` or a dedicated writer task).
  - **Status (2026-08-26): FIXED** — `EventStore::persist_async` /
    `mark_delivered_async` wrap the sync calls in `spawn_blocking`;
    completion is awaited on purpose (outbox persist-before-deliver order,
    delivered-mark must not race exit). `publish` uses the async persist,
    `handle_kernel_message` now takes `Option<&Arc<EventStore>>` and awaits
    `mark_delivered_async`; `run_retry_worker` batches the whole sweep
    (pending scan → retry bump → prune) into one blocking task per tick,
    preserving bump-before-redeliver order. Sync store methods unchanged for
    non-async callers; async round-trip regression test added. Same PR:
    `SdkHarness` gets a per-port `data_dir` — harness kernels previously
    shared the real user-dir `events.db`, so stale pending events from a
    timed-out test were redelivered by the retry worker into unrelated
    tests' subscribers (observed as cross-suite "cpp payload in python
    test" corruption).

### P2 — this cycle

- [x] UX-1 — **REST errors are bare `StatusCode` with no body/envelope
      (Medium):** 422 collapses distinct causes (invalid manifest vs spawn
      failure); `stop_plugin` returns 200 even when the stop failed
      (`routes.rs:115`); 429 is the only error with a body + `Retry-After`;
      WS upgrade failures are plain text; no OpenAPI — the contract lives
      only in `tests/unit/test_api.rs`.
  - Files: `src/api/routes.rs`, `src/api/middleware.rs`, `src/api/rate_limit.rs`.
  - Acceptance: a JSON error envelope (code/message/retryable) on all
    non-2xx; `stop_plugin` reports failure; endpoint doc (OpenAPI or README).
  - **Status (2026-09-11): SHIPPED (PR #85).**

- [x] S5 — **Internals leak into plugin-facing errors (Low):** registration
      reject sends raw jsonwebtoken detail (`auth failed: {e}`,
      `protocol.rs:322`); `ActionResponse.error` is a Debug enum name
      (`format!("{:?}", status)`, `protocol.rs:654`).
  - Files: `src/ipc/protocol.rs`.
  - Acceptance: stable, documented error codes/messages on the wire.
  - **Status (2026-09-11): SHIPPED (PR #87).**

- [x] UX-2 — **Debug repr leaks into public API shapes (Low-Med):**
      `PluginInfo.state = format!("{:?}", e.state)` (`routes.rs:59`) — a Rust
      Debug enum name is the public field.
  - Files: `src/api/routes.rs`.
  - Acceptance: stable documented string/enum values in the response.
  - **Status (2026-08-24): FIXED** — shared
    `registry::plugin_state_str()` (`"registered"`, lowercase like
    `device_state_str`) is now the single source; used by both REST sites
    and the kernel `list_plugins` command (same leak class). Locked by
    assertions in test_api/test_registry/kernel commands tests.

- [x] PERF-3 — **Per-message full `PluginEntry` clones + O(n) registry scans
      (Low-Med):** `registry.get` clones the whole entry incl. the manifest
      proto (`registry.rs:159`; ~4 per forwarded message via `get_by_conn_id`
      + `check_ipc_send` + `check_ipc_target` + `get`); `find_action_provider`
      (`:204-217`), `count_pending_actions_for` (`:339-344`),
      `find_pending_internal_id` per chunk (`:366-375`) scan linearly;
      broadcast clones all entries (`:185-190`).
  - Files: `src/plugins/registry.rs`, `src/auth/permissions.rs`.
  - Acceptance: `Arc<PluginEntry>` or split hot/cold fields; action→provider
    index; no O(n) scan per message.
  - **Status (2026-09-11): SHIPPED (PR #86).**

### P3 — backlog

- [x] PERF-4 — **Hot-path constant-factor costs (Low):** double CRC32 per
      outbound frame (build site + `write_frame_raw`); synchronous zstd
      compress/decompress on async threads (`wire/src/framing.rs:137-152,240`);
      sync `/proc` reads in the watchdog loop (`supervisor.rs:852,864`); WS
      double payload copy per frame (`websocket.rs:220,246-258`).
  - Files: `../vynkor-wire/src/framing.rs`, `src/plugins/supervisor.rs`,
    `src/api/websocket.rs`.
  - **Status (2026-09-11): SHIPPED.** CRC dedup in `write_frame_raw` (wire
    0.0.4): skip recompression when payload unchanged. Watchdog `/proc` and
    WS copy already closed (PR #68). zstd offload deferred (user decision).

- [x] UX-3 — **Config validation gaps + silent parse-error swallowing (Low):**
      unknown `restart:` silently → `on-failure` (`loader.rs:19-23`) while
      `max_fs_access` warns (two conventions); bad `log_level` → EnvFilter
      matches nothing → silent no-logs; binary defaults (port 8000) drift
      from the shipped config.yaml (8888); non-start CLI subcommands
      `.unwrap_or_default()` on load errors (`main.rs:85-123`).
  - Files: `src/plugins/loader.rs`, `src/utils/config.rs`, `src/main.rs`,
    `config.yaml`.
  - Acceptance: unknown `restart` warns; bad `log_level` warns and falls
    back to `info`; port defaults aligned; all CLI commands surface load
    errors.
  - **Status (2026-08-26): FIXED** — `config_from_def` warns on an
    unrecognized `restart:` value before falling back to `on-failure`
    (same convention as `max_fs_access`; regression-tested);
    `logging::sanitize_log_level` accepts only known level names, numeric
    1–5 or explicit `target=level` directives — anything else warns and
    falls back to `info` (a bare word otherwise becomes an implicit
    EnvFilter *target* directive and logs vanish); applied to both
    `try_init` and runtime `set_log_level`; `config.yaml` + README example
    aligned to the binary default `port: 8000`; the six non-start CLI
    commands propagate `load_config` errors instead of silently proceeding
    with defaults.

- [x] UX-4 — **CLI polish (Low):** sparse subcommand `about` text and a
      hardcoded version string (`cli/mod.rs`); mixed output style
      (✓/⚠/plain) and `vyn plugin logs` printing the raw JSON array
      (`cli/plugin.rs:135`).
  - Files: `src/cli/mod.rs`, `src/cli/plugin.rs`.
  - **Status (2026-08-24): FIXED** — version now `env!("CARGO_PKG_VERSION")`;
    every subcommand has a one-line clap `about`; `vyn plugin logs` parses
    the JSON array and prints one line per entry, falling back to verbatim
    output on unparsable bodies (`render_log_lines`, unit-tested).

- [x] S4 — **Dependency advisories (Low, warnings):** RUSTSEC-2026-0190
      `anyhow` `Error::downcast_mut` unsoundness; RUSTSEC-2025-0119
      `number_prefix` unmaintained.
  - Files: `Cargo.lock`, `Cargo.toml`.
  - **Status (2026-08-21): FIXED** — `cargo update -p anyhow` → 1.0.104;
    `indicatif` 0.17 → 0.18 (same `ProgressBar` API at the single call site)
    drops `number_prefix` from the tree entirely; bonus: h2 0.4.15 → 0.4.18
    clears RUSTSEC-2026-0258 (unbounded empty DATA frames, disclosed
    2026-08-17). `cargo audit` exits 0 — zero vulnerabilities, zero warnings.

---

## Immediate — Maintainability & code-quality audit (2026-08-20)

Findings from the manual full-`src/` audit (49 files, 14 251 LOC — see
`AUDIT.md` → "Full src Audit — 2026-08-20"). All are **OPEN** (2026-08-20),
kernel-local, and independent of each other unless noted. Priorities follow
the audit's §E: **P0** before next release (monoliths + error-system unification)
· **P1** hygiene (comments, config, test globals) · **P2** polish.

### P0 — before next release

- [x] MA-01 — **Split `ipc/protocol.rs` (1389 LOC) and
      `marketplace/registry.rs` (1509 LOC):** two files exceed the 250-LOC
      guideline 5–6×; `protocol.rs` bundles the router + 12 handlers
      (PluginRegister, ActionRequest, SessionClose, KernelCommand…);
      `registry.rs` bundles cache + fetch + verify + parse + resolve. Review
      is impractical at this size.
  - Files: `src/ipc/protocol.rs`, `src/marketplace/registry.rs`.
  - Acceptance: `protocol.rs` split into `ipc/router.rs` +
    `ipc/handlers/{register,action,session,event,kernel}.rs`; `registry.rs`
    split into `marketplace/registry/{cache,fetch,verify,parse}.rs` (or
    `#[cfg(test)] mod tests` moved out). Full suite + clippy + fmt green.
  - **Status (2026-09-11): SHIPPED (PR #82).**

- [x] MA-02 — **Extract duplicated frame/URL helpers:** `target_bytes` /
      `frame_target` / `build_frame` copied in 5 sites
      (`ipc/protocol.rs`, `bridge/mod.rs`, `events/bus.rs`,
      `plugins/supervisor.rs`, `api/websocket.rs`); `resolve_ws_url` /
      `resolve_advertise_url` / `resolve_relative_archive_urls` — 3 copies of
      URL resolution (`bridge/mod.rs`, `cli/device.rs`,
      `marketplace/registry.rs`). Any framing fix must be applied in 5 places.
  - Files: `src/ipc/protocol.rs`, `src/bridge/mod.rs`, `src/events/bus.rs`,
    `src/plugins/supervisor.rs`, `src/api/websocket.rs`, `src/cli/device.rs`,
    `src/marketplace/registry.rs`.
  - Acceptance: frame helpers live in `ipc/helpers.rs` or `vynkor_wire`; URL
    helpers in `utils/url.rs`; zero duplicated copies remain.
  - **Status (2026-08-26): FIXED** — re-inventoried post-F1 (marketplace
    fetcher now lives in vynkor-manager, so only two URL sites remain here).
    `protocol.rs`/`bus.rs` already imported the canonical
    `ipc::framing::build_frame`; the real duplicates were inline Frame
    constructions: connection.rs error-frame + both test frame builders and
    supervisor's watchdog ping now call `build_frame("client", 0, payload)`;
    bridge's test-local `frame_target` delegates to `framing::target_as_str`.
    New `utils/url.rs` is the single home for `DEFAULT_WS_PATH` +
    `ws_scheme_for` — bridge's `resolve_ws_url` and device's advertise path
    consume them instead of hardcoding `/ws` and the http→ws scheme map.
    Remaining crc32 call sites are not build_frame duplicates: websocket.rs
    inbound verification (MA-13 scope) and fragment reassembly (preserves
    original magic/flags/target).

- [x] MA-03 — **Unify the error system on `VynkorError`:** three error types
      coexist — `VynkorError`, `anyhow::Error`, and `Result<_, String>`
      (`auth/jwt.rs:58,83`). `jwt::validate()` returns `String`, breaking
      uniformity; `main.rs` formats `e.to_string()` and loses the error chain.
  - Files: `src/auth/jwt.rs`, `src/main.rs`.
  - Acceptance: `jwt::validate() -> Result<_, VynkorError>`; no `Result<_, String>`
    in error paths; `main.rs` preserves the error chain (e.g. `{:?}` or
    `Error::source()`).
  - **Status (2026-08-24): FIXED** — new `VynkorError::Auth(String)` variant;
    `jwt::validate()`/`mint_device_token()` return it with every message text
    preserved; `main.rs` start path formats config-load failures via `{e:#}`
    so the anyhow cause chain survives. PR #64.

- [x] MA-04 — **Replace deprecated `rand::thread_rng()`:** `auth/jwt.rs:96`
      uses the deprecated `rand::thread_rng()`; replace with `rand::rng()` /
      `OsRng`.
  - Files: `src/auth/jwt.rs`.
  - Acceptance: no `thread_rng()` calls; clippy clean.
  - **Status (2026-08-21): FIXED** — `mint_device_token` fills the jti nonce
    from `rand::rngs::OsRng`; no `thread_rng()` remains. The kernel pins rand
    0.8 where `rand::rng()` doesn't exist, so OsRng (valid on both) was used.

### P1 — hygiene

- [x] MA-05 — **Add `docs/COMMENT_TAGS.md` and reduce comment duplication:**
      audit tags (`T-11`, `S1`, `BUG-006`, `R9-02`…) are opaque without a
      glossary; the socket-0o600 rationale is duplicated 4×
      (`config.rs:272`, `ipc/server.rs:52`, `main.rs:479`, `utils/tls.rs:50`);
      comment/code ratio is ~1:1 in several files (every `Config` field has
      2–3 lines of docs, every `match` arm has 5 lines). `//` inline comments
      mix Capital+period and lowercase-no-period styles.
  - Files: new `docs/COMMENT_TAGS.md`, `src/utils/config.rs`,
    `src/ipc/server.rs`, `src/main.rs`, `src/utils/tls.rs`, tree-wide comment
    pass.
  - Acceptance: `docs/COMMENT_TAGS.md` maps every tag → issue → file;
    socket-0o600 rationale lives in `docs/SECURITY.md` (or similar) and
    in-code comments cross-reference it; `//` inline comments follow one
    convention (lowercase, no trailing period per `CLAUDE.md`); trivial
    restatements removed.
  - **Status (2026-09-11): SHIPPED (PR #89).**

- [x] MA-06 — **Replace `create_router_full(10 args)` with a config struct:**
      `api/server.rs`'s constructor is clippy-suppressed (`too_many_arguments`);
      `tokio::spawn(prune limiter)` inside the constructor spawns a background
      task with no `JoinHandle`, leaking in tests.
  - Files: `src/api/server.rs`.
  - Acceptance: `create_router_full` takes a `RouterConfig` struct; the prune
    task is spawned by `Kernel::run` (or returns a handle), not inside the
    constructor.
  - **Status (2026-08-24): FIXED** — `create_router_full(RouterConfig)` returns
    `BuiltRouter { app, rate_limiter }`; eviction spawned by `ApiServer::run`
    via exported `spawn_rate_limiter_prune`, join handle held for the server's
    lifetime; no handle-less task leaks in tests. PR #64.

- [x] MA-07 — **Fix `Config::Default` duplication + clamp all zero-invalid
      numerics:** `Default` hand-duplicates every `default_*()` fn — easy to
      desync; `clamp_invalid_numerics` (N3) clamps only 4 fields
      (`router_channel_capacity`, `max_connections`, `watchdog_*`), but
      `max_archive_bytes = 0` and `max_ws_connections = 0` are not clamped.
  - Files: `src/utils/config.rs`.
  - Acceptance: `Config` uses `#[derive(Default)]` + `#[serde(default = …)]`
    consistently (or `Default` delegates to the `default_*` fns); every
    zero-invalid numeric is clamped or errors loudly; tests cover all clamped
    fields.
  - **Status (2026-08-24): FIXED** — `Default` delegates to the `default_*`
    fns (`port`/`log_level` extracted, serde attrs wired); closes a live
    desync where serde used S2's `default_data_dir()` while `Default`
    hardcoded `/var/lib/vyn`. Clamps added for `max_ws_connections`/
    `max_archive_bytes`; tests cover all six clamped numerics. Surfaced a
    latent orchestrator bug: the caller's `EventBus` Arc was swapped for a
    store-backed clone whenever EventStore opened — replaced by set-once
    `EventBus::set_store` attach (Arc identity preserved). PR #64.

- [x] MA-08 — **Add `reset_for_test()` for global atomic sequences:**
      `MSG_SEQ`, `ACTION_CORRELATION_SEQ`, `EVENT_PUBLISH_SEQ`
      (`ipc/protocol.rs:30-32`) are process-wide and never reset; tests depend
      on ordering across runs.
  - Files: `src/ipc/protocol.rs`.
  - Acceptance: `#[cfg(test)] fn reset_for_test()` resets all three atomics;
    called in test setup where ordering matters.
  - **Status (2026-08-21): FIXED** — `#[cfg(test)]
    ipc::protocol::reset_for_test()` zeroes all three; locked in by
    `reset_for_test_zeroes_all_sequence_atomics`.

- [x] MA-09 — **Split `plugins/supervisor.rs` (933 LOC):** `spawn_internal`
      is 200 LOC + `monitor_loop` + `watchdog_loop` + `graceful_shutdown` in
      one file.
  - Files: `src/plugins/supervisor.rs`.
  - Acceptance: split into `supervisor/spawn.rs`, `supervisor/watchdog.rs`
    (or equivalent); full suite green.
  - **Status (2026-09-11): SHIPPED (PR #83).**

- [x] MA-10 — **Split `kernel/orchestrator.rs` (470 LOC):** bundles TLS
      resolve + `bind_ip` logic + bridge spawn + supervisor + watchdog +
      `disconnect_loop` ×2 + `graceful_shutdown`.
  - Files: `src/kernel/orchestrator.rs`.
  - Acceptance: split into `orchestrator/bind.rs`, `orchestrator/shutdown.rs`
    (or equivalent).
  - **Status (2026-09-11): SHIPPED (PR #84).**

### P2 — polish

- [x] MA-11 — **Extract `drain_to_log` and `proc_resource_usage` into
      `plugins/metrics.rs`:** these helper functions are misplaced in the
      supervisor.
  - Files: `src/plugins/supervisor.rs`, new `src/plugins/metrics.rs`.
  - Acceptance: helpers moved; supervisor imports them; tests green.
  - **Status (2026-08-21): FIXED** — both moved to `plugins::metrics`
    (verbatim, incl. their doc comments); supervisor imports them,
    `proc_resource_usage` import stays linux-gated. Suite green.

- [x] MA-12 — **Log mutex poison instead of silently swallowing it:**
      `unwrap_or_else(|p| p.into_inner())` (e.g. `events/store.rs`) discards
      the poison error silently.
  - Files: `src/events/store.rs` (+ any other `into_inner()` sites).
  - Acceptance: poison is logged at `warn!` before recovery.
  - **Status (2026-08-21): FIXED** — shared `utils::sync::recover_poison`
    (`warn!` then recover); all 14 `into_inner()` sites across
    `events/store.rs`, `api/websocket.rs`, `ipc/connection.rs`,
    `ipc/server.rs`, `bridge/mod.rs` now pass it to `unwrap_or_else`.

- [x] MA-13 — **Reuse `vynkor_wire` framing in the WebSocket gateway:**
      `api/websocket.rs:229` has a custom `parse_frame` without
      `COMPRESSED`/`FRAGMENTED` support — any framing fix must be applied in
      two places.
  - Files: `src/api/websocket.rs`.
  - Acceptance: WS gateway reuses `vynkor_wire::framing::read_frame` (or the
    WS-specific framing moves into `vynkor_wire`); no duplicated frame parser.
  - **Status (2026-09-11): SHIPPED (PR #88).**

- [x] MA-14 — **Reduce `utils/logging.rs` duplication + use `try_init()`:**
      4 `if json { with otel } else` branches duplicate 80% of `fmt::layer()`;
      `Registry::init()` panics on a second call (breaks tests).
  - Files: `src/utils/logging.rs`.
  - Acceptance: shared `fmt::layer()` builder; `try_init()` instead of
    `init()` so tests don't panic.
  - **Status (2026-08-21): FIXED** — the json/plain choice is made once into
    a boxed fmt layer (`Box<dyn Layer<BaseStack>>`, `BaseStack` =
    `Layered<reload::Layer<EnvFilter, Registry>, Registry>`), so the field
    config is no longer duplicated per branch and the otel tail composes
    after it; `init()` renamed to `try_init() -> bool` (returns false when a
    global subscriber is already installed instead of panicking); all five
    `main.rs` call sites updated. Builds with and without the `otel` feature.

- [x] MA-15 — **Check `vynkor-wire` for dead code:** `cargo clippy -- -D
      warnings` on the `vynkor-wire` workspace may flag `dead_code` (e.g.
      `BLOOM`).
  - Files: `../vynkor-wire/`.
  - Acceptance: `cargo clippy --all-targets -- -D warnings` clean on
    `vynkor-wire`.
  - **Status (2026-08-21): FIXED** — no dead_code found (`BLOOM` doesn't
    exist in wire); the actual `-D warnings` blocker was
    `clippy::large_enum_variant` on the generated `envelope::Payload` oneof,
    silenced via prost `type_attribute` in wire's build.rs (vynkor-wire
    PR #5). clippy `--all-targets --all-features -- -D warnings` clean.

- [x] MA-16 — **Separate tests from prod code in `registry.rs`:** `registry.rs`
      is ~800 LOC prod + ~700 LOC tests in one file — hard to scroll. Move
      tests to `#[cfg(test)] mod tests` in a separate file (or `tests.rs`).
  - Files: `src/marketplace/registry.rs`.
  - Acceptance: prod and test code separated; `cargo test` still discovers
    all tests.
  - **Status (2026-08-21): FIXED** — tests moved verbatim to
    `src/marketplace/registry_tests.rs`, wired via
    `#[cfg(test)] #[path = "registry_tests.rs"] mod tests;` (keeps
    `registry.rs` a file module, no directory reshuffle); `registry.rs` drops
    to 665 LOC of prod code. All 41 registry tests still discovered, full
    suite green.

### Security nits (confirmed sound, low priority)

- [x] MA-17 — **Unify `validate_slug` / `validate_plugin_id` regex:**
      `installer.rs:614` and `registry.rs:547` use two different regexes for
      the same concept.
  - Files: `src/marketplace/installer.rs`, `src/marketplace/registry.rs`.
  - Acceptance: one shared `validate_slug` function; both call-sites use it.
  - **Status (2026-08-21): FIXED** — shared `utils::validate::
    validate_identifier(id, max_len)` (charset/length/path-component gate);
    `installer::validate_slug` delegates and keeps its slug wording,
    `registry::validate_plugin_id` delegates and keeps the `kernel`/`*`
    reserved checks. Side effect: `"."`/`".."` are now rejected as plugin
    ids too (previously only slugs).

- [x] MA-18 — **Add `jwt_secret` length check to `mint_device_token`:**
      `jwt_secret` length is validated only in `orchestrator.rs:123`;
      `mint_device_token` (`src/cli/token.rs`) does not check it.
  - Files: `src/cli/token.rs`, `src/auth/jwt.rs`.
  - Acceptance: `mint_device_token` rejects a short `jwt_secret` (same
    `MIN_JWT_SECRET_BYTES` threshold).
  - **Status (2026-08-21): FIXED** — the constant moved to
    `auth::jwt::MIN_JWT_SECRET_BYTES` (single source of truth, orchestrator
    imports it); `mint_device_token` enforces it before minting, so `vyn
    token mint` / `vyn device pair` can no longer sign with a brute-forceable
    secret. Test SECRET bumped to 32 bytes;
    `mint_device_token_rejects_short_secret` covers 31/32 boundary.

- [x] MA-19 — **Add `debug_assert!` + safety comment to `unsafe` in
      `main.rs:391`:** `BorrowedFd::borrow_raw(ready_fd)` is valid only because
      `ready_fd` is dup'd via `CommandExt::pre_exec` — the safety invariant is
      undocumented.
  - Files: `src/main.rs`.
  - Acceptance: `debug_assert!` + `// SAFETY:` comment explaining the
    `borrow_raw` validity.
  - **Status (2026-08-21): FIXED** — `// SAFETY:` documents that ready_tx
    outlives spawn() so the borrow never dangles and grants no ownership;
    `debug_assert!(ready_fd >= 0)` added before the unsafe block.

---

## Immediate — Dumb-core audit (2026-08-16)

Boundary audit of the kernel against the Manifesto ("dumb byte router +
process supervisor") — verdict: *declared, partially drifted*. Findings are
mirrored as DC-1…DC-5 in `AUDIT.md`; the full fix plan and §7 decisions live
in `docs/DUMB_CORE_AUDIT.md`. All items below are **OPEN** (2026-08-16).
Priorities per the audit §6: **P0** = F1/F2 (clearest violations, unblock the
rest) · **P1** = F3/F4/F5/F6 (this cycle).

| Priority | Items | Source |
|----------|-------|--------|
| P0 | F1, F2 | `docs/DUMB_CORE_AUDIT.md` §6 |
| P1 | F3, F4, F5, F6 | `docs/DUMB_CORE_AUDIT.md` §6 |

- [x] F1 (DC-1, P0) — **Extract the marketplace out of the kernel:**
  `src/marketplace/` no longer ships in the `vyn` binary.
  **SHIPPED 2026-08-22** — standalone repo
  [`vynkor-manager`](https://github.com/vynkor-core/vynkor-manager) (`vynm`),
  manifest module in vynkor-wire 0.2.4–0.2.6 behind the `manifest` feature,
  kernel marketplace deleted in vynkor PR #43; `vyn plugin install/search/…`
  are delegation shims to `vynm`. Task breakdown: `docs/VYNM_ROADMAP.md`
  (V-01…V-07 done; V-08 closes stage 2).
  - **Authoritative plan: `docs/VYNM_PLAN.md`** (finalized 2026-08-21 —
    separate repo `vynkor-manager`, manifest module in vynkor-wire behind a
    feature, independent versioning, multi-source registries with optional
    keys). The decision below is kept for history; where it differs from
    VYNM_PLAN, VYNM_PLAN wins.
  - Decision (§7): standalone binary **`vynm`** ("vyn manager") — no new
    kernel surface, writes the same `plugins.d/` drop-ins the kernel already
    reads; UX: `vynm install|search|list|remove|enable|disable`; new
    code/docs use **vynkor** naming.
  - Files: `src/marketplace/` (`registry.rs`, `installer.rs`, `state.rs`),
    `src/cli/plugin.rs`, `src/cli/complete.rs`, `Cargo.toml` (drop
    `zip`/`indicatif` if unused elsewhere), new `vynm` binary/crate.
  - Acceptance: `vyn` contains no marketplace code; `vynm install` works
    standalone; `database`/`secrets` still install and run against a kernel
    with no marketplace module; marketplace unit tests move with it.

- [x] F2 (DC-2, P0) — **Keep device surfaces as dumb pass-through, move
  interpretation:** the kernel keeps identity + liveness + raw metadata and
  exposes them as observability (same shape as `GET /plugins`); interpretation
  and friendly UX live outside (a `discovery` plugin / web frontend).
  - Files: `src/plugins/registry.rs` (`device_os_str`/`device_state_str`
    display mapping `:511-529` — reduce to raw wire values; `devices` map and
    the online/offline transition `:239-247` stay), `src/api/routes.rs:107-136`,
    `src/kernel/commands.rs:61-78`, `src/cli/devices.rs`.
  - Acceptance: `GET /devices` stays and returns raw pass-through data; no
    interpretation helpers in the kernel; a `discovery` plugin provides the
    friendly view; device integration tests pass unchanged (no API break).
  - **Status (2026-09-11): SHIPPED (PR #91).**

- [x] F3 (DC-2, P1) — **Keep the bridge as transport, strip capability
  interpretation:** the `role: client` bridge stays in the kernel as transport
  (remote connectivity, symmetric to the WS gateway); only `device.<cap>`
  mirroring semantics move out to the remote agent.
  - Files: `src/bridge/mod.rs` (810 L), `src/cli/device.rs`,
    `src/cli/token.rs`, `src/utils/config.rs` (`Role::Client`,
    `BridgeConfig`).
  - Acceptance: the bridge still connects a client kernel to a host; no
    capability semantics in the kernel; the Android agent (vynkor) still pairs
    via the existing tooling; no `BridgeConfig` change needed.
  - **Status (2026-09-11): SHIPPED (PR #92).**

- [x] F4 (DC-3, P1) — **Neutralize the AI tool-calling surface (generic
  manifest feature):** `action_specs`/`get_manifest` stay in the protocol as a
  generic per-action capability mechanism with the "for the AI" framing
  removed (comments/wording only — no wire break, no feature removal).
  - Files: `../vynkor-wire/proto/vynkor_protocol.proto:159-173`,
    `src/kernel/commands.rs:79-127` (`get_manifest`),
    `src/events/bus.rs:223-259`.
  - Acceptance: no "for the AI"/"to the AI"/"AI" references in the protocol
    schema or kernel comments for this mechanism; behavior unchanged; all
    tests green.
  - **Status (2026-09-11): SHIPPED (this branch).**

- [x] F5 (DC-4, P1) — **Drop the hardcoded action→permission fallback
  (three-step migration):** the kernel has no knowledge of any specific
  plugin's actions; the data-driven v2 path is the single source of truth.
  Step 1 is a hard dependency: the `network` plugin declares
  `action_requirement: http_request → network` in its v2 manifest first.
  - Files: `src/auth/permissions.rs:12-17`, `src/ipc/protocol.rs:652-653`,
    `src/plugins/loader.rs:74-90`, `src/plugins/registry.rs:282-300`,
    `docs/PLUGIN_REGISTRY_SCHEMA.md:264,294`.
  - Acceptance: no plugin/action-name strings in `src/auth/`; a v2 action
    without a declared permission is **denied by default**; legacy string-form
    plugins keep working with a boot warning.
  - **Status (2026-09-11): SHIPPED (this branch).** Temporary deprecation
    warning kept for backward compatibility; easy to remove later.

- [x] F6 (DC-5, P1) — **Manifesto wording + event-store hardening:** the "no
  databases" clause says what it means (event-delivery outbox carve-out); the
  SQLite outbox is safe (S2) and off the async hot path (PERF-2).
  - Files: `README.md` §1, `ROADMAP.md` Manifesto, `src/events/store.rs`,
    `src/events/bus.rs`, `src/utils/config.rs` (`data_dir` default),
    `config.yaml`.
  - Acceptance: manifesto wording matches reality; `events.db` lives in a
    0o700 private dir; the publish path performs no synchronous SQLite I/O;
    event-delivery integration tests stay green.
  - **Status (2026-08-26): SHIPPED.** Wording landed (`2f47d5f`: README §1 +
    Manifesto carve-out); S2 shipped (PR #35, 2026-08-18); PERF-2 shipped
    (PR #70 — `spawn_blocking` wrappers + batched retry sweep); retention
    documented (README "bounded 1h retention" + `event_retention_secs`).
    Integration tests green throughout.

---

## Phase 9 — Hard isolation (deferred)

Deferred until the R8 cross-repo items ship. The current sandbox (`sandbox:
true`) isolates via user + network namespaces plus rlimits only — plugins
share the host PID space (they can enumerate every host process via
`/proc`), read/write host files as the real uid, and their `max_procs`
budget is *shared with every other process of the same uid* (RLIMIT_NPROC
is checked at each fork/clone against the real-uid thread count, walked up
the entire user-namespace tree to `init_user_ns`). Phase 9 replaces the
shared-uid rlimit accounting with cgroup accounting and closes the
visibility/file-system gaps.

- [x] R9-01 — **Per-plugin process accounting via cgroup v2 `pids.max`
      instead of RLIMIT_NPROC:** today a plugin's thread budget is the
      host-wide real-uid count (desktop sessions routinely run ~700+
      threads, so `max_procs` is a *shared* budget, not per-plugin
      isolation — a thread storm in one plugin or in the desktop starves
      the other). The cgroup v2 `pids` controller counts tasks *inside the
      cgroup only*: supervisor creates a `vynkor/<plugin_id>.scope` cgroup,
      writes the child PID into `cgroup.procs` in `pre_exec`, and sets
      `pids.max = max_procs`. Requires cgroup v2 (systemd default) and
      either root or a delegated `user@1000.service` subtree.
  - Files: `src/plugins/runner.rs` (`sandbox_pre_exec`), `src/plugins/supervisor.rs`.
  - Acceptance: a plugin with `max_procs: 64` runs on a host whose session
    already uses 700+ threads (currently a hard EAGAIN boundary); a
    thread-storm in one plugin does not consume another plugin's budget.
  - Done: per-plugin scopes land in the first writable ancestor that has
    the `pids` controller (root-first walk, falls back to RLIMIT_NPROC
    when none); scope reaped on exit. Covered by
    `tests/unit/test_supervisor.rs`: `pids_cgroup_accounts_plugin_threads_per_plugin`,
    `sandboxed_plugin_still_joins_its_pids_cgroup` (join from inside the
    user namespace), `thread_storm_in_one_plugin_does_not_consume_another_budget`.

- [x] R9-02 — **PID-namespace isolation via shim supervisor:** the current
      spawn path cannot combine `unshare(CLONE_NEWPID)` with threading —
      the exec'd plugin would inherit a pending `pid_for_children`
      namespace and every thread spawn fails with EINVAL (documented in
      `runner.rs`). Correct approach: a shim process breaks the coupling —
      kernel forks a tiny wrapper, the wrapper unshares `CLONE_NEWPID` and
      forks the plugin (which is *born* into the namespace as PID 1, where
      threads work), then forwards signals and exit status. Effect: the
      plugin sees only its own processes (`/proc` shows just itself), and
      can no longer enumerate or signal host/other-plugin processes.
  - Files: new `src/plugins/shim.rs`, `src/plugins/supervisor.rs`.
  - Acceptance: a plugin's `/proc` lists only its own tasks; `ps` inside
    the plugin shows one process; supervisor signal/exit forwarding still
    works (restart, SIGTERM shutdown).
  - Done: `plugins::shim` — a single-threaded re-exec of our own binary
    (hidden `vyn __shim`, dispatched in `main` before the tokio runtime
    exists, since `unshare(CLONE_NEWUSER)` fails in a multithreaded
    process). It nests a user namespace (works without CAP_SYS_ADMIN),
    unshares `CLONE_NEWPID|CLONE_NEWNS`, makes `/` private (no proc
    propagation onto the host), remounts a fresh `/proc` bound to the new
    PID namespace, and forks the plugin as PID 1. A socketpair readiness
    gate fail-closes the spawn: the supervisor only gets the plugin's host
    pid after the plugin signalled from inside the sandbox, so a plugin
    that could not enter the sandbox is killed and never runs unisolated.
    The shim forwards TERM/INT/HUP and mirrors the exit status (restart and
    graceful shutdown are unchanged). A handler-less plugin — PID 1 of its
    namespace — silently drops unhandled signals (`SIGNAL_UNKILLABLE`), so
    the shim escalates a forwarded TERM/INT/HUP to SIGKILL once the grace
    period elapses (`VYNKOR_SHIM_GRACE_SECS`, default 5s, taken from the
    plugin's `grace_seconds`); the supervisor's `child.wait()` always
    returns and the pids scope is reaped. `pdeathsig=SIGKILL` is set on
    both shim and plugin. The supervisor's wait task runs the orphan-gap
    sweep *before* `cleanup_pids_cgroup` — a shim killed outright (watchdog
    SIGKILL, SIGKILL deadline) never reaps the plugin — and retries the
    rmdir briefly so a just-SIGKILLed zombie cannot leak the scope. All
    lifecycle signals target the shim (`PluginEntry::signal_target`);
    watchdog SIGKILL goes to the shim too, which dies and takes the
    namespace with it. The
    shim binary is overridable via `VYNKOR_SHIM_BIN` (tests). Covered by
    `tests/integration/test_shim.rs`: `sandboxed_plugin_sees_only_its_own_pid_namespace`,
    `shim_forwards_sigterm_to_sandboxed_plugin`,
    `shim_reports_exit_status_for_supervision`.

- [x] R9-03 — **Filesystem isolation (Landlock LSM first, minimal rootfs
      later):** plugins currently read/write the host filesystem with the
      real uid's credentials. Landlock (Linux 5.13+, unprivileged,
      `no_new_privs`-compatible) lets `pre_exec` restrict file access
      declaratively — read-only access to declared dirs, write access only
      to the plugin's data dir — with no CAP_SYS_ADMIN requirement.
      Heavier alternative if stricter containment is needed: mount
      namespace + `chroot`/`pivot_root` into a minimal rootfs (plugin
      binary + runtime libs + data dir only), which the userns already
      grants CAP_SYS_ADMIN for.
  - Files: `src/plugins/runner.rs`, config schema (`max_fs_access` /
    `readonly_paths` / `writable_paths`).
  - Acceptance: a plugin denied read access to `~` gets EACCES on open;
    plugin writes are confined to its declared writable dirs.
  - Done: `plugins::fsaccess` builds and enforces the Landlock ruleset via
    the `landlock` crate (`landlock = "0.4.7"`, linux-gated) in the shim's
    plugin `pre_exec` — before the readiness byte, so a plugin that cannot
    be restricted is killed and never runs unrestricted (fail-closed).
    Config: `max_fs_access: full|read-only|none` (default `full`), with
    `readonly_paths` (granted read+execute on dirs) and `writable_paths`
    (full read/write). The ruleset always grants: the plugin binary's own
    dir (resolved execvp-style, `resolve_binary_path`), system lib dirs
    (`/usr/lib`, `/usr/lib64`, `/lib`, `/lib64`), `/etc/ld.so.cache`, and
    `ResolveUnix` on the kernel UDS path (ABI v9; the plugin must still
    reach the kernel to register). Restricted kernels downgrade gracefully
    via the crate's best-effort compat; `NotEnforced` fails the spawn.
    Only enforced when `sandbox: true` — otherwise a warning is logged.
    Covered by `tests/unit/` (`fsaccess` module: mode/env parsing, rule
    access-right mapping, path resolution) and `tests/integration/test_shim.rs`
    (`sandboxed_plugin_denied_undeclared_reads`,
    `sandboxed_plugin_writes_only_declared_writable_paths`,
    `sandboxed_plugin_reads_declared_readonly_paths`,
    `sandboxed_plugin_full_mode_is_unrestricted`,
    `sandboxed_plugin_reaches_kernel_socket`).

- [x] R9-04 — **seccomp syscall filter in `pre_exec`:** default-deny
      allowlist (or a tight denylist of kernel-escape-capable syscalls —
      `ptrace`, `bpf`, `keyctl`, `reboot`, `kexec_load`, `mount`/`umount`
      while R9-03 is unlanded) applied before exec, so a compromised
      plugin cannot use exotic syscalls to attack the kernel. Needs a
      runtime profiling pass per SDK (tokio/Python/CPP baseline) so the
      allowlist doesn't break legitimate plugins.
  - Files: `src/plugins/runner.rs`.
  - Acceptance: a plugin calling a denied syscall gets `EPERM`/`SIGSYS`;
      all three SDK example plugins still pass their integration tests.
  - Done: `plugins::seccomp` — a **tight denylist** of kernel-escape-
    capable syscalls enforced via the `seccompiler` crate (pure-Rust BPF
    compiler, no system libseccomp) in the shim's plugin `pre_exec`,
    after Landlock and before the readiness byte (fail-closed: a plugin
    that cannot be filtered is killed and never runs unfiltered).
    Denied: `ptrace`, `bpf`, `keyctl`/`add_key`/`request_key`, module
    loading (`init_module`/`finit_module`/`delete_module`), `reboot`,
    `kexec_load`/`kexec_file_load`, mount-namespace escape (`mount`,
    `umount2`, `pivot_root`, `chroot`, `setns`, `open_tree`,
    `move_mount`, `fsopen`, `fsconfig`, `fspick`, `mount_setattr`),
    file-handle Landlock bypass (`open_by_handle_at`/`name_to_handle_at`),
    cross-process memory (`process_vm_readv`/`process_vm_writev`),
    `perf_event_open`, `userfaultfd`, `io_uring_*`, `kcmp`,
    `swapon`/`swapoff`, `acct`, `syslog`, `sethostname`/`setdomainname`,
    `modify_ldt`, `quotactl`, `lookup_dcookie`, `vhangup`,
    `fanotify_init`. Everything else stays allowed (a default-deny
    allowlist would need per-SDK maintenance that rots when runtimes
    change; the deny set is stable by construction). Action is
    `SIGSYS` (`KillProcess`) — the plugin dies instead of retrying.
    Baselines from a ptrace-based tracer across Rust/tokio, CPython and
    the C++ SDK confirm no legitimate syscall is denied; a sandboxed
    plugin calling `ptrace` dies with SIGSYS (smoke + integration
    tests: `sandboxed_plugin_denied_ptrace_dies_with_sigsys`,
    `sandboxed_plugin_runs_normally_under_seccomp`, unit tests for the
    deny-set coverage).

- [x] R9-05 — **Interim process-visibility hardening (until R9-02):** in a
      new mount namespace (`CLONE_NEWNS`, permitted inside the userns),
      remount `/proc` with `hidepid=2` so plugins cannot read other
      processes' cmdlines/environ while the shared-PID-space limitation is
      still in force.
  - Files: `src/plugins/runner.rs`.
  - Acceptance: `/proc/<other-pid>/` resolves to ENOENT for non-namespace
      processes; plugin functionality unaffected.
  - Superseded by R9-02: sandboxed plugins now live in a private PID
      namespace with a fresh `/proc` bound to it — host processes are not
      enumerable at all, which is strictly stronger than `hidepid=2`. No
      interim work landed; the item is closed with R9-02.

- [x] R9-06 — **Docs: fix stale isolation references + record exact rlimit
      semantics:** README §5's "tracked in `AUDIT.md`" for the shim
      supervisor is stale — the item lives here (Phase 9), fix the pointer.
      Document that `max_procs` is a *shared real-uid* budget, not
      per-plugin (RLIMIT_NPROC is checked per fork/clone against the
      real-uid thread count and the check walks the userns tree up to
      `init_user_ns`), and that the sandbox does not yet hide host
      processes or restrict file access. Fold R9-01's cgroup migration in
      as the eventual correct accounting.
  - Files: `README.md` §5, `src/plugins/runner.rs` doc comments,
    `config.yaml` (Vynkor repo) `max_procs` comment.
  - Acceptance: no stale `AUDIT.md` pointers; README accurately states
    what the sandbox does and does not isolate today.
  - Done: README §5 rewritten — the `AUDIT.md` pointer is gone, the
    sandbox's isolation (user + network + PID + mount namespaces, fresh
    `/proc`, `pids.max` accounting, RLIMIT caps) and its non-goals
    (no file-access restriction — R9-03, no seccomp — R9-04) match the
    code. `max_procs` semantics are documented on `PluginConfig` in
    `src/plugins/supervisor.rs` (shared real-uid budget at clone time,
    per-plugin when a `pids` cgroup scope is writable — R9-01).

---

## Phase 10 — Plugin config & marketplace state (deferred)

Deferred until the R8/N items ship. Today a plugin's runtime settings live
inline in `config.yaml` (`plugins:` list), the installer edits that one shared
file with marker-comment blocks (`# vynkor install:` /
`append_config_example` / `remove_config_example`), and marketplace state is
split between the TTL `registry.json` cache and whatever happens to exist on
disk under `~/.local/lib/vyn/plugins/`. Fine for 5 plugins, the bottleneck
once the fleet grows: one shared file owned by nobody in particular, text-block
surgery, and no record of what is installed, from where, or at what version.
Phase 10 gives each plugin its own config file and gives the marketplace an
explicit state store. Independent of Phase 9 — can land before or after it.

- [x] R10-01 — **Per-plugin config via `plugins.d/` drop-in directory:** the
      `plugins:` list leaves `config.yaml` for a `plugins.d/*.yaml` directory
      (new `plugins_dir` config key, default `<config_dir>/plugins.d/`); each
      file carries exactly one plugin entry (id, binary, restart, sandbox,
      limits, env). `load_config` globs and merges the files — order = filename
      sort, duplicate `id` across files = boot error, SIGHUP reload merges the
      same way. `vyn plugin install` writes `plugins.d/<slug>.yaml`,
      `vyn plugin remove` deletes it — the marker-block machinery
      (`append_config_example`/`remove_config_example`) is deleted with them.
  - Files: `src/utils/config.rs`, `src/marketplace/installer.rs`,
    `src/cli/plugin.rs`, `config.yaml` (Vynkor repo).
  - Acceptance: install/remove never touch `config.yaml`; a hand-written
    entry in `plugins.d/` boots; duplicate ids across files fail loudly.
  - Done: `Config.plugins_dir` (default `<config dir>/plugins.d/`,
    `resolve_plugins_dir` shared by boot + CLI);
    `merge_plugin_dropins` globs `*.yaml`/`*.yml`, filename-sort merge after
    the inline `plugins:` list, duplicate `id` across drop-ins or with the
    inline list = boot error. Installer: marker machinery deleted, replaced
    by `write_plugin_config`/`remove_plugin_config` — `create_new`
    (O_CREAT|O_EXCL) so a pre-planted symlink can never redirect the write
    (M-09 class), and `validate_slug` (`[A-Za-z0-9._-]`, no separators)
    applied to `write`/`remove`/`uninstall` so operator or registry input
    cannot traverse out of `plugins.d/`/`plugin_dir`. CLI `install` writes
    `plugins.d/<slug>.yaml` (existing file left untouched, with a note),
    `remove` deletes it and warns if the id is still configured. Repo
    `config.yaml` stripped of the inline `plugins:` + marker blocks; the
    five dev plugins moved to `plugins.d/*.yaml`. Inline `plugins:` still
    parses (deprecated) so existing configs keep booting. SIGHUP re-reads +
    re-merges (parse errors/dups surface at reload) but does not respawn the
    plugin set — documented. Covered by `src/utils/config.rs` unit tests
    (merge order, default/explicit `plugins_dir`, dup inline+drop-in / two
    drop-ins, missing dir, full `PluginDef` round-trip) and
    `tests/unit/test_installer.rs` (write/remove drop-ins, sandbox hint,
    existing-file no-clobber, symlink no-follow, traversal rejection in
    write/remove/uninstall). Live: boot loads all five `plugins.d/` plugins
    and they register.

- [x] R10-02 — **Explicit installed-plugin state store:** replace
      filesystem-sniffing `~/.local/lib/vyn/plugins/<slug>` with
      `~/.local/share/vyn/installed.json` recording slug, version, sha256,
      install time, source registry URL. Enables `vyn plugin list --installed`
      offline (no registry fetch), upgrade detection (installed vs registry
      version), and a `remove` that works even when the plugin dir is missing
      or half-deleted.
  - Files: `src/marketplace/installer.rs` (new state module),
    `src/cli/plugin.rs`.
  - Acceptance: `vyn plugin list --installed` shows versions offline;
    reinstalling the same version warns instead of re-extracting; remove
    tolerates a missing dir.
  - Done: `marketplace::state` — `installed.json` under the XDG data dir
    (`VYNKOR_STATE_DIR`/`XDG_DATA_HOME`/`$HOME/.local/share/vyn`, mirroring
    `plugin_dir()`'s env-override pattern), written atomically (temp + rename),
    corrupt file tolerated (logs + starts empty). `install` records
    slug/version/archive-sha256/install-time/registry-source after a success
    and skips a same-version reinstall whose dir still exists (missing dir
    falls through to repair); `uninstall` drops the state record and succeeds
    when the dir is already gone. `vyn plugin list --installed` prints the
    offline table (slug/version/installed-at/source). Covered by
    `tests/unit/test_state.rs` (15 tests: roundtrip, upsert, remove, missing-
    dir tolerance, reinstall-skip matrix, timestamp formatting). Live-verified
    against the real registry: install → state → kernel spawns the
    marketplace-installed plugin → remove (dir + state) → remove with dir
    deleted by hand.

- [x] R10-03 — **`registry.json` cache rework:** the TTL cache at
      `~/.cache/vyn/registry.json` is a raw mirror of the remote registry
      document. Move it under the marketplace state dir, version the schema,
      persist per-plugin `installed_version`/`last_check`, and make
      signature/revocation handling explicit (stale entry policy decided and
      tested) instead of implicit TTL-only expiry.
  - Files: `src/marketplace/registry.rs`, `src/marketplace/state.rs` (new).
  - Acceptance: cache file carries a schema version; revoked-entry handling
    is explicit and covered by tests.
  - Done: the cache moved to `registry-cache.json` in the marketplace state
    dir (`state_dir()`), wrapped in a versioned
    `RegistryCache{schema_version, last_check, meta, entries, plugins}` with
    atomic temp+rename writes; a foreign/missing `schema_version` or corrupt
    file reads as empty. Registry v2 readiness: the parser accepts both the
    flat array and the v2 map form (`{meta, revoked, "<slug>": {versions}}`,
    see vynkor-plugins ROADMAP "Infrastructure Evolution") via an untagged
    shape, flattening `versions` into one entry per version and folding the
    root `revoked` list into each entry's `status` — so only
    `RegistryEntry::is_revoked()` exists downstream. **Stale policy
    (decided):** the cache only ever persists entries whose maintainer
    signature verified at write time (`verify_entries`, pinned or
    `marketplace_public_key` override) — a stale fallback therefore never
    serves unverified content; an all-unverified refetch (compromised
    channel / wrong key) keeps the previous verified snapshot instead of
    clobbering it. **Revocation:** `status: revoked` (flat form) or the v2
    root `revoked: ["slug", "slug@ver"]` list; revoked entries stay cached —
    revocation outlives the TTL — and `install` refuses them with a clear
    error; `vyn plugin list` marks them `[revoked]`. Per-plugin
    `installed_version`/`last_check` are snapshotted from `installed.json`
    at write for offline upgrade detection. The kernel-side change is
    independent of when vynkor-plugins ships the v2 document — absent
    `status`/`meta` fields read as `stable`/`None`.
  - Covered by `src/marketplace/registry.rs` unit tests (versioned
    round-trip, v2 parse + revoked-list folding, unverified-not-cached,
    keep-previous-on-all-unverified, revocation-outlives-TTL,
    per-plugin snapshot, foreign schema = empty) and
    `tests/unit/test_installer.rs`
    (`install_refuses_revoked_entry`). Live-verified against the real
    registry: fetch → cache with `schema_version: 1` in the state dir, 4/4
    entries verified; offline stale fallback with a dead network still
    lists.

- [x] R10-04 — **`vyn plugin enable|disable <slug>`:** toggling a per-plugin
      file (rename/comment-out) replaces hand-editing `config.yaml` when an
      operator wants a plugin kept on disk but not auto-spawned — today that
      means uncommenting/commenting the installer's block by hand, which
      `remove_config_example` then deliberately refuses to touch.
  - Files: `src/cli/plugin.rs`, `src/plugins/loader.rs`.
  - Acceptance: `disable` stops auto-spawn on boot without uninstalling;
    `enable` restores it; the state survives SIGHUP reload.
  - Done: `installer::disable_plugin_config`/`enable_plugin_config` rename
    `plugins.d/<slug>.yaml` ↔ `<slug>.yaml.disabled` — the rename (not a
    delete) preserves the operator's tuning, so re-enabling restores it
    verbatim. A `Toggle` outcome distinguishes toggled/already/missing;
    `validate_slug` guards the path (M-09 class); an active+disabled file
    pair is refused because `fs::rename` would silently clobber the disabled
    copy. CLI: `vyn plugin disable|enable <slug>`; a missing drop-in errors
    with `PluginNotFound`, or notes when the slug lives in the deprecated
    inline `plugins:` list (where a rename can't stop it). The loader needed
    no change — `merge_plugin_dropins` only globs `*.yaml`/`*.yml`, so the
    renamed file is skipped at boot and on SIGHUP reload. Covered by 9
    installer unit tests (`test_installer.rs`) plus
    `load_config_skips_disabled_dropin` (`config.rs`); live-verified:
    enabled → kernel spawns the plugin, disabled → no spawn on boot, and
    the double-toggle / unknown-slug error paths are clean.

---

## Phase 11 — Protocol v1.4: permission additions

The `secrets` plugin is the first that cannot ship without the new permission
values, so P11 gates the whole next plugin batch. Tracked from the plugin side
in `vynkor-plugins/ROADMAP.md` ("Kernel-side changes needed"); this section is
the kernel's half of the same work. Purely an enum addition + regeneration +
copy sync — no new Envelope payloads, no IPC/framing/orchestrator changes
(the existing `ActionRequest`/`Event`/`EventPublish`/IPC/streaming/WS
surfaces cover every planned plugin).

- [x] P11-01 — **Add five `PermissionType` values (15–19, contiguous):**
  `PERMISSION_SECRETS` (15), `PERMISSION_CLIPBOARD` (16),
  `PERMISSION_LAUNCH` (17), `PERMISSION_SCREEN` (18), `PERMISSION_HOME`
  (19); bump the `// v 1.3` header to `// v 1.4`. Contiguity is
  load-bearing: `known_permissions()` (`src/marketplace/installer.rs:23`)
  probes enum codes and stops after 4 consecutive misses, so a gap ≥4
  silently rejects plugins declaring later values. Regenerate the
  `vynkor-wire` prost types — `known_permissions()` (R8-01) and the JWT
  `permissions` claims (free-form strings) adopt the new values with no
  Rust source change. Bump `vynkor-wire` to 0.3.0 and drop the crates-io
  patch override if one is in effect, per the R8-07 precedent.
  - Files: `wire/proto/vynkor_protocol.proto`, `vynkor-wire/` (regenerate
    + publish).
  - Acceptance: a plugin.json declaring `"secrets"` passes
    `validate_manifest`; R8-02's drift test stays green.
  - **Status (2026-08-13): SHIPPED** — proto v1.4 landed in `vynkor-wire`
    (`899bf8d`), regen'ed prost types consumed by the kernel
    (`0b98dac`, merged via PR #13 `31f2cd4`); R8-02's drift test stays
    green. Note: `vynkor-wire` published as **0.2.1** (not 0.3.0). The
    `[patch.crates-io]` override pinned wire **0.2.2**
    (`feat/wire-v1.5-status-renumber`, the P11-03 renumber below) and
    `vynkor-sdk 0.1.3` until both published (2026-08-13); the override was
    then dropped — see P11-03.

- [x] P11-02 — **Sync all six proto copies (fixes pre-existing v1.2
  drift):** the three in-repo copies (`wire/proto`,
  `sdk/python/proto`, `sdk/cpp/proto`) are R8-05-guarded and move
  together; the standalone `vynkor-sdk-python`/`vynkor-sdk-cpp` repos are
  **already behind on v1.2** — missing `PERMISSION_EVENT_PUBLISH`,
  `PERMISSION_STORAGE`, the R6 streaming messages
  (`ActionRequestChunk`/`ActionResponseChunk`/`ActionStreamAbort`/
  `SessionClose`), `EventPublish*`, and `ActionRequest.caller_plugin_id` —
  and have no drift guard. Sync all six to v1.4 in one pass; extend R8-05
  (or add a release-time check) to cover the standalone copies so they
  can't drift again.
  - Files: all six `vynkor_protocol.proto` copies,
    `tests/unit/test_proto_sync.rs`.
  - Acceptance: Python/C++ SDK examples exercise `publish_event` and
    storage-permission manifests against a v1.4 kernel.
  - **Status (2026-08-13): SHIPPED** — all three sibling copies
    (`../vynkor-wire`, `../vynkor-sdk-python`, `../vynkor-sdk-cpp` proto
    files) are byte-identical at v1.4; R8-05 was extended with a staleness
    check on the generated Python binding asserting the five new
    permission names plus `caller_plugin_id`, so a regen skip fails loudly.

- [x] P11-03 — **Land M9 (zero-value enum renumber) on the next wire-breaking
  protocol bump.** Deferred by decision — `AUDIT.md:242`: "bundle into the
  next wire-breaking protocol version bump; do not fix piecemeal". The
  interim lint guarding explicit `status:` at construction sites is already
  in place since Phase 6 (T-16); this is the full fix.

  **Why (the bug it fixes):** proto3 defaults every enum field to its `0`
  value whenever the wire omits it. `ActionStatus` and `CommandStatus` are
  the only status enums in the protocol that make `0` = **success**
  (`ACTION_OK = 0`, `COMMAND_OK = 0`), so any construction site that forgets
  `set_status()` — or any peer built from an older/drifting proto copy —
  silently reads back as **OK** instead of an error. Every other status enum
  in the file already follows the safe `*_UNKNOWN = 0` pattern:
  `PermissionType` (`PERMISSION_UNKNOWN = 0`), `EventPublishStatus`
  (proto line 270 explicitly documents it: "a missed set_status() shows up
  as this, not OK"), `ErrorCode` (`ERR_UNKNOWN = 0`), `AudioCodec`
  (`AUDIO_CODEC_UNSPECIFIED = 0`). `ActionStatus` does not even *have* an
  `ACTION_UNKNOWN` variant; `CommandStatus` parks `COMMAND_UNKNOWN` on 2
  instead of 0. Renumbering flips the default to "unknown", so a missed
  `set_status()` fails loudly downstream instead of faking success.

  **Current → target** (in `../vynkor-wire/proto/vynkor_protocol.proto`):

  | `ActionStatus` | now | after | | `CommandStatus` | now | after |
  |---|---|---|---|---|---|---|
  | `ACTION_UNKNOWN` | — | **0** (new) | | `COMMAND_UNKNOWN` | 2 | **0** |
  | `ACTION_OK` | 0 | 1 | | `COMMAND_OK` | 0 | 1 |
  | `ACTION_ERROR` | 1 | 2 | | `COMMAND_ERROR` | 1 | 2 |
  | `ACTION_TIMEOUT` | 2 | 3 | | `COMMAND_PERMISSION_DENIED` | 3 | 3 |
  | `ACTION_PERMISSION_DENY` | 3 | 4 | | | | |
  | `ACTION_NOT_FOUND` | 4 | 5 | | | | |
  | `ACTION_QUOTA_EXCEEDED` | 5 | 6 | | | | |
  | `ACTION_STREAM_BACKPRESSURE` | 6 | 7 | | | | |

  Result: every status enum in the file has `0` = "unknown/unset". Do **not**
  touch the already-correct enums (`PermissionType`, `EventPublishStatus`,
  `ErrorCode`, `AudioCodec`) — renumbering them is a gratuitous break.

  **Where** (every location that must move in lockstep — one wire-breaking
  version, no partial landings):
  - `../vynkor-wire/proto/vynkor_protocol.proto` — the renumber itself;
    header `// v 1.4` → `// v 1.5`.
  - `../vynkor-wire/src/lib.rs` — `PROTOCOL_VERSION` `"1.4"` → `"1.5"`,
    same commit as the header (they must stay in sync).
  - `../vynkor-wire/Cargo.toml` — `0.2.1` → `0.3.0`: breaking wire changes
    bump the **minor** (additive changes bump patch), per the wire README.
  - `../vynkor-sdk-python/proto/vynkor_protocol.proto`,
    `../vynkor-sdk-cpp/proto/vynkor_protocol.proto` — re-sync byte-identical
    (R8-05 reads these sibling paths directly and fails on drift).
  - `../vynkor-sdk-python/vynkor/vynkor_protocol_pb2.py` — regenerate via
    `../vynkor-sdk-python/scripts/gen_proto_python.py`. Caveat: the R8-05 staleness marker check
    asserts symbol **names**, not values, so a pure renumber with a skipped
    regen would NOT fail loudly — the regen must be done deliberately.
  - `../vynkor-sdk-rust/Cargo.toml` — `vynkor-wire = "0.2.1"` → `"0.3.0"`.
  - `Cargo.toml` (this repo) — keep the `[patch.crates-io]` override until
    wire 0.3.0 publishes, then drop the wire entry (the `vynkor-sdk 0.1.3`
    entry stays until that crate publishes).
  - **No Rust/C++/Python source edits anywhere**: every construction and
    comparison site uses the named variant (`ActionStatus::ActionOk as i32`,
    `r.status() == vynkor::proto::ACTION_OK`, `ActionStatus.ACTION_OK`,
    `set_status(ACTION_OK)`), so the new values arrive via the regenerated
    bindings. (Verified 2026-08-13: kernel `src/ipc/protocol.rs`,
    `src/kernel/commands.rs`, integration tests, and all three SDKs contain
    zero hardcoded status numbers.)

  **How (release order):**
  1. `vynkor-wire`: renumber the two enums, bump header + `PROTOCOL_VERSION`
     + `Cargo.toml` to 0.3.0 in **one commit**; `cargo build` regenerates the
     prost types.
  2. Re-sync the two vendored proto copies (python/cpp) byte-identical and
     regenerate `vynkor_protocol_pb2.py` (step is deliberate — see caveat).
  3. `vynkor-sdk-rust`: bump the `vynkor-wire` requirement to 0.3.0.
  4. Publish `vynkor-wire` 0.3.0 to crates.io.
  5. Kernel: full suite — R8-02/R8-05 and the T-16 interim lint must stay
     green; drop the wire patch entry once 0.3.0 is on crates.io.
  6. Grep the SDKs for residual numeric status comparisons (`== 0`, `== 1`
     against a status field) before closing — a renumber can't be caught by
     the compiler.

  **Acceptance:** `ACTION_OK`/`COMMAND_OK` are nonzero; every status enum in
  the file has `*_UNKNOWN = 0`; R8-02/R8-05 and the interim lint
  (`action_response_and_command_ack_literals_set_status_explicitly`,
  `tests/unit/test_proto.rs`) all pass; Python/C++ examples round-trip
  against a v1.5 kernel.

  **Status (2026-08-13): SHIPPED** — v1.5 landed on
  `feat/wire-v1.5-status-renumber`: `ActionStatus` gains `ACTION_UNKNOWN = 0`
  (OK/ERROR/... shift to 1..7), `CommandStatus` moves `COMMAND_UNKNOWN` to 0
  (OK/ERROR → 1/2). Header + `PROTOCOL_VERSION` + `Cargo.toml` moved in one
  commit; the python/cpp proto copies were re-synced byte-identical and
  `vynkor_protocol_pb2.py` regenerated (deliberate — R8-05 asserts symbol
  *names*, not values). **Deviation from the plan:** `vynkor-wire` stayed at
  **0.2.2** (not 0.3.0); `vynkor-sdk-rust` needed no bump (its `0.2.1` req
  was satisfied by the 0.2.2 patch). No source edits anywhere — every status
  construction and comparison site uses named variants (re-grepped across
  kernel + all three SDKs; the C++ `echo_plugin` was rebuilt against the
  v1.5 bindings). R8-02 /
  R8-05 and the T-16 interim lint stay green; full suite passes (91 unit +
  84 integration + 260 api), clippy `-D warnings` and `fmt --check` clean.

---

## Cross-repo coordination

- **vynkor-plugins** (`vynkor-plugins/ROADMAP.md`): database plugin landing
  (R8-06) depends on R8-01/R8-02 landing here first — the kernel must accept
  `PERMISSION_STORAGE` before the registry entry is installable. The
  protocol v1.4 permission additions (Phase 11) are likewise tracked from
  the plugin side in its "Kernel-side changes needed" section — `secrets`
  was the first plugin blocked on P11; with P11-01/P11-02 shipped the kernel
  side is unblocked; P11-03 (M9) shipped on v1.5 and does not gate plugins.
- **vynkor-wire** (`vynkor-wire/`): 0.2.0 publish (R8-07) shipped; the
  protocol v1.4 bump (P11-01) shipped as **0.2.1** on crates.io; the v1.5
  status-enum renumber (P11-03) shipped as **0.2.2** on crates.io
  (2026-08-13). `vynkor-sdk` **0.1.3** (wire req 0.2.2) published the same
  day; the kernel's `[patch.crates-io]` override was dropped — everything
  resolves from the registry.
- **vynkor-sdk-python / vynkor-sdk-cpp** (standalone repos): proto copies
  synced to v1.4 (P11-02) and guarded — the R8-05 drift test reads the
  sibling-repo paths directly and now also checks the generated Python
  binding for staleness.
- **sdk/python/proto**, **sdk/cpp/proto** vendored copies: guarded by the R8-05
  drift test, which reads them via sibling-repo paths (`../vynkor-sdk-python`,
  `../vynkor-sdk-cpp`) after the submodule removal below.
- **Submodules removed (temporary decision, 2026-08-11):** `sdk/*` and `wire/`
  are no longer git submodules of this repo. The kernel consumes `vynkor-wire`
  and `vynkor-sdk` from crates.io; cross-SDK integration tests and the proto
  drift guard read sources from the sibling repos (`../vynkor-wire`,
  `../vynkor-sdk-cpp`, `../vynkor-sdk-python`), which CI checks out itself.
  **Revisit in the future:** decide between a true monorepo, restored
  submodules, or published-artifact-only consumption — see the trade-offs
  (lockstep proto iteration vs. release cadence) recorded at removal time.

## Task Summary

| Item | Scope | Depends on |
|------|-------|------------|
| R8-01 | installer permissions derived from `PermissionType` enum | none |
| R8-02 | permission drift-detection tests | R8-01 |
| R8-03 | runtime `check_permission` normalization | none |
| R8-04 | registry schema doc alignment | none |
| R8-05 | proto-copy byte-identity test | none |
| R8-06 | `database` plugin landing (vynkor-plugins) | R8-01, R8-02 |
| R8-07 | `vynkor-wire` 0.2.0 publish — shipped; patch override re-added for the v1.4 housekeeping bump (see P11-01: wire entry droppable, `vynkor-sdk 0.1.3` entry stays) | none |
| N1 | router payload-sharing (`Arc<[u8]>`) in `forward`/`broadcast` — closed as non-issue; `Arc::ptr_eq` regression tests | none |
| N2 | permission form normalization in clamp + config cross-check — shipped, tests for both forms | none |
| N3 | config numeric bounds validation — shipped, zero-clamp + warn + tests | none |
| N4 | daemon-start readiness handshake (pid-file TOCTOU) — shipped, smoke-verified | none |
| N5 | `cargo fmt` fix for `test_proto_sync.rs` (DoD gate) — shipped, gate green | none |
| M7 | C++/Python framing fuzz harness (deferred) | none |
| M9 | zero-value enum renumber — SHIPPED with protocol v1.5 (P11-03, 2026-08-13) | v1.5 wire bump |
| R9-01 | cgroup v2 `pids.max` per-plugin accounting (replaces shared-uid RLIMIT_NPROC) | R8 + N ship gate |
| R9-02 | PID namespace via shim supervisor | R9-01 |
| B1 | stop/start race — stale `ExitEvent` (no PID/epoch) → wrong-instance restart → duplicate registration — shipped: epoch-gated `ExitEvent`, stale exits dropped | R9-02 |
| B2 | `stop` swallows ESRCH, can orphan the live registered instance — shipped: stop blocks until exit (SIGKILL deadline) | R9-02 |
| B3 | `spawn_internal` overwrites the manual-start entry on duplicate restart — shipped: `PluginAlreadyRunning` guard + restart-cancel on stop | R9-02 |
| B4 | cgroup scope reap loops on `Device or resource busy` — shipped: stop waits for reap completion | R9-02 |
| R9-03 | filesystem isolation (Landlock / minimal rootfs) — shipped: Landlock ruleset in shim pre_exec, `max_fs_access`/`readonly_paths`/`writable_paths` config, fail-closed, integration-tested | R9-02 |
| R9-04 | seccomp syscall filter — shipped: tight kernel-escape denylist via `seccompiler` in shim pre_exec, fail-closed, SIGSYS on denied syscalls, SDK baselines profiled, regression tests | R9-03 |
| R9-05 | `/proc` `hidepid=2` interim visibility hardening | R8 + N ship gate |
| R9-06 | docs: fix stale `AUDIT.md` pointer, record exact rlimit semantics | none |
| R10-01 | plugin settings out of `config.yaml` → `plugins.d/` drop-in dir — shipped: merge + `plugins_dir` key, write/remove drop-ins, slug/symlink hardening, inline list deprecated | R8 + N ship gate |
| R10-02 | installed-plugin state store (`installed.json`) — shipped: XDG data-dir ledger, atomic writes, reinstall-skip, missing-dir-tolerant remove, offline `list --installed` | none |
| R10-03 | `registry.json` cache rework — shipped: versioned `registry-cache.json` in the state dir, verified-entries-only stale policy, `revoked` status blocks install, registry v2 map-form parsing | R10-02 |
| R10-04 | `vyn plugin enable\|disable` toggle — shipped: drop-in rename to `<slug>.yaml.disabled` (skipped by the `*.yaml` glob at boot + SIGHUP), content preserved on re-enable, slug/path hardening, inline-`plugins:` note | R10-01 |
| P11-01 | protocol v1.4 — `PermissionType` additions 15–19 (`SECRETS`/`CLIPBOARD`/`LAUNCH`/`SCREEN`/`HOME`), header bump, wire regeneration — shipped (`899bf8d` + `31f2cd4`); `vynkor-wire` published as 0.2.1; `vynkor-sdk 0.1.3` published 2026-08-13, `[patch.crates-io]` dropped | `secrets` plugin (vynkor-plugins) needs it |
| P11-02 | proto-copy sync — all sibling copies byte-identical at v1.4 + Python-binding staleness check — shipped | P11-01 |
| P11-03 | M9 zero-value enum renumber — SHIPPED on protocol v1.5 (2026-08-13): `*_UNKNOWN = 0` for ActionStatus/CommandStatus, header + `PROTOCOL_VERSION` 1.5, `vynkor-wire` 0.2.2 consumed via patch branch (crates.io publish deferred), python/cpp copies synced + pb2 regenerated, no source edits anywhere | v1.5 wire bump |
| S1 | registry signature must bind the full entry (`status`/`archive_url`/compat) — revocation bypass + download redirect — **FIXED** (2026-08-14): full-message signature, resolution moved to install (after verification), cache schema v2, tamper regression tests | none |
| S3 | `crossbeam-epoch` 0.9.18 → 0.9.20 (RUSTSEC-2026-0204) — **FIXED** (2026-08-14, PR #20) | none |
| S2 | `data_dir` off shared /tmp + 0o700 store dir — **FIXED** (2026-08-18, PR #35) | none |
| PERF-1 | router kernel replies off the shared-task `.send().await` — **FIXED** (2026-08-26): sync `try_send` replies, drop+counter on full channel, regression-tested | none |
| PERF-2 | event-store SQLite off the async runtime (`spawn_blocking`) — **FIXED** (2026-08-26): async store wrappers + batched retry sweep; SdkHarness data_dir isolation kills cross-test events.db reuse | none |
| UX-1 | JSON error envelope + honest stop status + API doc — **SHIPPED** (2026-09-11, PR #85) | none |
| S5 | stable wire error codes, no internals — **SHIPPED** (2026-09-11, PR #87) | UX-1 |
| UX-2 | stable `PluginInfo.state` values — **FIXED** (2026-08-24): shared lowercase `plugin_state_str`, all sites | UX-1 |
| PERF-3 | `Arc<PluginEntry>` + action→provider index — **SHIPPED** (2026-09-11, PR #86) | none |
| PERF-4 | hot-path constant factors — **SHIPPED** (2026-09-11): CRC dedup in wire 0.0.4; watchdog `/proc` + WS copy already closed | vynkor-wire 0.0.4 |
| UX-3 | config validation consistency + surface load errors to all CLI — **FIXED** (2026-08-26): restart/log_level warn+fallback, port aligned, CLI errors propagated | none |
| UX-4 | CLI help/output polish — **FIXED** (2026-08-24): version from cargo env, clap `about` everywhere, `plugin logs` renders line-per-entry | none |
| S4 | dependency advisories (anyhow / number_prefix) — **FIXED** (2026-08-21): anyhow 1.0.104, h2 0.4.18, number_prefix dropped via indicatif 0.18 — cargo audit clean | none |
| MA-01 | split `ipc/protocol.rs` + `marketplace/registry.rs` monoliths — **SHIPPED** (2026-09-11, PR #82) | MA-02 |
| MA-02 | extract duplicated `target_bytes`/`build_frame` + `resolve_*_url` helpers — **FIXED** (2026-08-26): inline Frame builds → `ipc::framing::build_frame`, `utils/url.rs` owns `DEFAULT_WS_PATH` + ws-scheme map | none |
| MA-03 | unify error system on `VynkorError`; `jwt::validate() -> VynkorError` — **FIXED** (2026-08-24): `VynkorError::Auth`, jwt paths unified, main.rs chain preserved (PR #64) | none |
| MA-04 | replace deprecated `rand::thread_rng()` — **FIXED** (2026-08-21): jti nonce from OsRng | none |
| MA-05 | `docs/COMMENT_TAGS.md` + reduce comment duplication + consistent style — **SHIPPED** (2026-09-11, PR #89) | none |
| MA-06 | `create_router_full` → `RouterConfig` struct; move prune spawn out — **FIXED** (2026-08-24): `RouterConfig`/`BuiltRouter`, prune owned by `ApiServer::run` (PR #64) | none |
| MA-07 | `Config::Default` dedup + clamp all zero-invalid numerics — **FIXED** (2026-08-24): Default delegates to `default_*` fns, all clamps covered (+ EventBus set-once store attach fix) (PR #64) | none |
| MA-08 | `reset_for_test()` for global atomics (`MSG_SEQ` etc.) — **FIXED** (2026-08-21): `#[cfg(test)]` reset + regression test | none |
| MA-09 | split `plugins/supervisor.rs` (933 L) — **SHIPPED** (2026-09-11, PR #83) | none |
| MA-10 | split `kernel/orchestrator.rs` (470 L) — **SHIPPED** (2026-09-11, PR #84) | none |
| MA-11 | move `drain_to_log`/`proc_resource_usage` → `plugins/metrics.rs` — **FIXED** (2026-08-21): helpers moved verbatim, supervisor imports them | none |
| MA-12 | log mutex poison instead of silently swallowing — **FIXED** (2026-08-21): shared `utils::sync::recover_poison`, all 14 sites | none |
| MA-13 | reuse `vynkor_wire` framing in WS gateway; drop custom `parse_frame` — **SHIPPED** (2026-09-11, PR #88) | none |
| MA-14 | `utils/logging.rs` dedup + `try_init()` — **FIXED** (2026-08-21): one boxed fmt layer, `try_init()` no longer panics on re-init | none |
| MA-15 | `vynkor-wire` dead-code clippy check — **FIXED** (2026-08-21): large_enum_variant allowed on generated payload oneof (wire PR #5) | none |
| MA-16 | separate tests from prod code in `registry.rs` — **FIXED** (2026-08-21): tests moved verbatim to `registry_tests.rs`, wired via `#[path]`; registry.rs at 665 LOC prod | MA-01 |
| MA-17 | unify `validate_slug`/`validate_plugin_id` regex — **FIXED** (2026-08-21): shared `utils::validate::validate_identifier`; `"."`/`".."` now rejected as plugin ids too | none |
| MA-18 | `mint_device_token` length-checks `jwt_secret` — **FIXED** (2026-08-21): constant moved to `auth::jwt`, enforced at every mint site | none |
| MA-19 | `debug_assert!` + SAFETY comment on `unsafe` in `main.rs:391` — **FIXED** (2026-08-21) | none |
| F1 | marketplace out of the kernel → standalone `vynm` binary (DC-1) — **SHIPPED** 2026-08-22 (`vynkor-manager` + vynkor PR #43) (P0, 2026-08-16 dumb-core audit) | none |
| F2 | device surfaces as dumb pass-through; interpretation moves to a `discovery` plugin (DC-2) — **SHIPPED** (2026-09-11, PR #91) | none |
| F3 | bridge stays as transport; strip `device.<cap>` capability interpretation (DC-2) — **SHIPPED** (2026-09-11, PR #92) | F2 |
| F4 | neutralize AI tool-calling surface → generic manifest feature (DC-3) — **SHIPPED** (2026-09-11, proto + kernel comments) | none |
| F5 | drop hardcoded action→permission fallback (DC-4) — **SHIPPED** (2026-09-11, temp warning kept) | network plugin v2 manifest (vynkor-plugins) |
| F6 | manifesto wording + event-store hardening (DC-5) — **SHIPPED** 2026-08-26: wording (`2f47d5f`) + S2 (PR #35) + PERF-2 (PR #70); DC-5 closed | none |

**Ship gate:** R8-01..R8-05 are kernel-local and land together on `develop`;
R8-06/R8-07 are cross-repo coordination items shipped from their own repos.
The Immediate N1–N5 items shipped (2026-08-11) — all kernel-local, independent
of the cross-repo items; N5 restored the DoD `fmt` gate. Phase 9 was explicitly
deferred until R8 shipped; with that gate lifted, R9-01 (cgroup pids), R9-02
(shim PID namespace), R9-05 (closed with R9-02), R9-06 (docs), and R9-03
(Landlock filesystem isolation) have shipped on `develop`. R9-04 (seccomp)
shipped with the tight kernel-escape denylist (2026-08-12) — Phase 9 is now
complete. M7 remains deferred by decision; M9 shipped with protocol v1.5
(P11-03, 2026-08-13). R9-01/R9-05 are
Linux-cgroup/mount-namespace work and require a delegated cgroup v2 subtree or
root. Phase 10 (plugin config + marketplace state) is likewise deferred and
independent of Phase 9 — it can land before or after hard isolation.
 Phase 11 shipped (2026-08-13): P11-01 (protocol v1.4 permission values 15–19,
 `vynkor-wire` 0.2.1 published) and P11-02 (proto-copy sync + drift guard) are
 done; P11-03 (M9 zero-value enum renumber) shipped on protocol **v1.5** —
 `vynkor-wire` **0.2.2** and `vynkor-sdk` **0.1.3** published to crates.io the
 same day; the `[patch.crates-io]` override in `Cargo.toml` was dropped and the
 workspace resolves both from the registry.

## Phase 12 — Remote devices (foundation)

Task breakdown: `docs/REMOTE_DEVICES_ROADMAP.md` (design + decisions in
`docs/REMOTE_DEVICES_PLAN.md`). Local-first stays the default — remote devices
are an additive deployment on top of the single-machine kernel, never a
migration. Dumb core stays dumb: the kernel stores/passes device metadata and
tool schemas, never interprets them. Build order: **D-01 → D-02 → D-03 →
D-04** (identity + versioning + discovery, one proto bump), then D-05 → D-07
(client kernel + transport).

- [x] **D-01 — Proto v1.6: device identity + versioning + `user_id` + tool
  schema (one additive bump).** `PluginRegister` += `device_id`, `os`
  (`DeviceOs`), `arch`, `os_version`, `capabilities[]`, `protocol_version`
  (semver), `user_id`; `PluginManifest` += `platforms[]` + `action_specs[]`
  (`ActionSpec { name, description, params_schema, risk,
  requires_confirmation }`); new `DeviceInfo`/`DeviceState`/`ActionRisk`.
  `PROTOCOL_VERSION` 1.5 → 1.6, `vynkor-wire` 0.2.2 → 0.2.3 (patch —
  additive), header + const + `Cargo.toml` in one commit.
  - Files: `../vynkor-wire/proto/vynkor_protocol.proto`,
    `../vynkor-wire/src/lib.rs`, `../vynkor-wire/Cargo.toml`, vendored
    copies (`../vynkor-sdk-python`, `../vynkor-sdk-cpp`), regenerated
    `vynkor_protocol_pb2.py`, `tests/unit/test_proto_sync.rs`.
  - Acceptance: regen compiles; copies byte-identical; drift test green;
    header/`PROTOCOL_VERSION`/`Cargo.toml` bumped in one commit.
  - **Status (2026-08-14): SHIPPED** — wire PR #4 (proto v1.6 + 0.2.3 +
    value-lock tests), sdk-python #3 (proto + regenerated pb2), sdk-cpp #3
    (proto), kernel PR #22 (R8-05 v1.6 markers + new header/const pairing
    test). All merged 2026-08-14; full kernel suite green (444 tests),
    `clippy -D warnings` + `fmt --check` clean. `vynkor-wire` **0.2.3
    published to crates.io 2026-08-14**. Kernel still consumes published
    wire **0.2.2** — D-03 wires the fields up and bumps the dep to 0.2.3
    (registry resolve, no `[patch.crates-io]` needed).

- [x] **D-02 — Registry: device/user identity + `devices` map + `last_seen`.**
  `PluginEntry` += `device_id`/`user_id` (defaults `"local"`/`"default"`);
  new `devices: DashMap<device_id, DeviceInfo>` populated at registration;
  `last_seen` advances on ping/pong (reuses `pong_times`); a device flips
  `Offline` when its last plugin leaves; `get_device`/`list_devices`
  exposed for the D-04 discovery surface.
  - Files: `src/plugins/registry.rs`.
  - Acceptance: registration stores device/user; `devices` populated;
    `last_seen` advances on ping/pong.
  - **Status (2026-08-14): SHIPPED** — kernel PR #23 (`543c758`), merged
    into develop. Full suite green (450 tests, `clippy -D warnings`,
    `fmt --check`). `DeviceInfo` is a kernel-local record shape-compatible
    with proto v1.6 — the kernel still runs wire 0.2.2 (proto v1.5), so
    D-03 bumps the dep to 0.2.3 and swaps in the wire type; the router
    still passes `""`/`""` (host plugins → default device) until D-03
    parses the wire fields.

**Delta audit (2026-08-14):** 13 new findings, all kernel-local, with
priorities P0–P3 (see "Immediate — Delta audit findings" above). As of
2026-08-26 only three remain open: **UX-1**, **S5** (both P2, S5 gated on
UX-1), **PERF-3** (P2) — plus the wire-crate remainder of **PERF-4**
(CRC dedup + zstd offload, needs a `vynkor-wire` release). Shipped: S1
(2026-08-14), S3 (PR #20), S2 (PR #35), PERF-1 (PR #68), PERF-2 (PR #70),
UX-2/UX-4 (2026-08-24), UX-3 (PR #68); PERF-4 partial (PR #68).

## Definition of Done

- `cargo test --all --all-features` exits 0; new behavior has regression tests.
- `cargo clippy --all-targets --all-features -- -D warnings` clean; `cargo fmt --check` clean.
- C++: existing CMake test targets stay green; new tests follow the
  `sdk/cpp/tests/test_*.cpp` naming/registration pattern in `CMakeLists.txt`.
- Python: new tests follow the `tests/test_*.py` pattern in the
  `vynkor-sdk-python` repo (unit tests live in the SDK, not the kernel;
  kernel-side cross-SDK integration tests stay in `tests/integration/`).
- Docs updated in the same PR (README for operator-visible changes; no
  `docs/FRAMING.md` changes expected since the wire format doesn't change).
