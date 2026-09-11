# Plugin Registry Schema

Normative reference for `registry.json` (central marketplace index) and `plugin.json` (per-plugin
manifest). All tooling (`vyn install`, `vyn plugin list/search`) and the kernel load-time gate are
derived from this document.

---

## registry.json — Central Plugin Registry

**Canonical URL:**

```
https://raw.githubusercontent.com/vynkor-core/vynkor-plugins/main/registry.json
```

Top-level structure: a JSON array of plugin entries.

```json
[
  { ... },
  { ... }
]
```

### Entry Schema

```json
{
  "id": "001",
  "slug": "stt-whisper",
  "name": "Whisper STT",
  "description": "Speech-to-text via OpenAI Whisper. Supports 16kHz mono PCM input.",
  "version": "1.2.0",
  "permissions": ["audio_stream", "network"],
  "archive_url": "https://github.com/vynkor-core/vynkor-plugins/releases/download/stt-whisper-1.2.0/stt-whisper-1.2.0.zip",
  "source_url":  "https://github.com/vynkor-core/vynkor-plugins/releases/download/stt-whisper-1.2.0/stt-whisper-1.2.0-src.zip",
  "sha256": "<64-char lowercase hex>",
  "min_kernel_version": "0.3.0",
  "max_kernel_version": "1.0.0",
  "signature": "<128-char lowercase hex Ed25519 signature>",
  "status": "stable"
}
```

### Field Rules

| Field | Type | Rules |
|-------|------|-------|
| `id` | string | Zero-padded 3-digit numeric string (e.g. `"001"`). Monotonically increasing. Never reused after deletion. |
| `slug` | string | Pattern `[a-z0-9-]+`. Globally unique. Stable across versions — same slug always refers to the same plugin. |
| `name` | string | Human-readable display name. No length limit. |
| `description` | string | One-sentence summary. Used by `vyn plugin search`. |
| `version` | string | Semver of the released archive (`MAJOR.MINOR.PATCH`). |
| `permissions` | string[] | Lowercase permission names without `PERMISSION_` prefix, matching names in `PermissionType` enum. The `PERMISSION_`-prefixed proto form (e.g. `PERMISSION_STORAGE`) is also accepted. |
| `archive_url` | string | HTTPS URL to the `.zip` binary archive. |
| `source_url` | string | HTTPS URL to the `.zip` source archive. Required for audit; may be same as `archive_url` for source-only plugins. |
| `sha256` | string | SHA-256 of the archive bytes at `archive_url`, as 64-char lowercase hex. Verified before extraction. |
| `min_kernel_version` | string | Semver lower bound (inclusive). `vyn install` rejects if running kernel is older. |
| `max_kernel_version` | string | Semver upper bound (inclusive). `vyn install` rejects if running kernel is newer. Use `"*"` for no upper bound. |
| `signature` | string | Ed25519 signature (128-char lowercase hex, 64 bytes) over the full canonical entry `"{slug}:{version}:{sha256}:{status}:{archive_url}:{min_kernel_version}:{max_kernel_version}:{permissions}"` (S1 fix 2026-08-14), produced by the offline maintainer signing key. `status` and `archive_url` are now bound to the signature — tampering either (flipping `revoked → stable`, or redirecting the download URL) fails verification. Verified against a pinned public key (or `marketplace_public_key` in `config.yaml` for private registries) *independent of* the `sha256` check — a compromised registry-serving channel can lie about both the archive and its hash together, but cannot forge this signature (T-11). |
| `status` | string | Optional. Lifecycle status: `stable` (default when absent), `beta`, `deprecated`, `hidden`, `revoked`. Only `revoked` is enforced by the kernel: `vyn install` refuses a revoked entry, and the entry stays listed with a `[revoked]` marker so an operator sees why. Revocation is enforced via the signature: flipping `revoked → stable` fails verification (S1 fix 2026-08-14). `is_revoked` is checked after verification. |

### Invariants

- Every `id` appears exactly once in the array.
- Every `slug` appears exactly once in the array.
- `min_kernel_version` must be a valid semver string or absent (treated as `"0.0.0"`).
- `max_kernel_version` must be a valid semver string or `"*"`.
- `min_kernel_version <= max_kernel_version` when both are semver (not `"*"`).
- A `status` of `revoked` is terminal: the entry is never installable, and no version of the same `slug` should be published until the incident is resolved under a new version.

### Kernel-side registry cache (R10-03)

The kernel mirrors the fetched document to `registry-cache.json` in the
marketplace state dir (same directory as `installed.json`; `VYN_STATE_DIR` (legacy `VYNKOR_STATE_DIR` still read)
/ `XDG_DATA_HOME` relocate it). The cache is a **versioned wrapper**, not a
raw mirror:

- `schema_version` — a cache written with a different version is read as
  empty (never misread); the file is written atomically (temp + rename).
- `last_check` + per-slug `installed_version`/`last_check` — inputs for
  offline upgrade detection.
- `meta` — echoed from the registry document when present (registry v2).

**Stale policy:** the cache only ever persists entries whose maintainer
signature verified at write time (against the pinned key or the
`marketplace_public_key` override). A stale-cache fallback on network failure
therefore never serves unverified content; an all-unverified refetch
(compromised channel / wrong key) keeps the previous verified snapshot.
**Revocation outlives the TTL:** a `revoked` entry stays in the cache and is
refused by `install` even when the cache is stale.

### Registry v2 (planned — parser already tolerant)

The vynkor-plugins roadmap ("Infrastructure Evolution") plans to reshape
`registry.json` into an object keyed by slug with a root `meta` and per-version
delivery metadata:

```json
{
  "meta": { "apiVersion": 2, "lastUpdated": "2026-08-13" },
  "revoked": ["evil@1.0.0"],
  "ai": {
    "name": "AI",
    "status": "stable",
    "versions": {
      "0.1.0": {
        "archive_url": "https://.../ai.zip",
        "sha256": "<hex>",
        "signature": "<hex>",
        "min_kernel_version": "0.1.0",
        "max_kernel_version": "*"
      }
    }
  }
}
```

The kernel parser accepts **both** the flat array and this map form (snake_case
or camelCase field names); `versions` flatten to one entry per version, and a
root `revoked` list folds into each matching entry's `status`. A plugin entry
with no `versions` yet produces no installable entry. This is forward
compatibility only — no kernel change is required when the v2 document ships.

---

## plugin.json — Plugin Manifest

Every **marketplace-installed** plugin directory must contain a `plugin.json` at its root
(`vyn install` validates it in Step 7). For config-declared local plugins the file is
**optional**: when present the kernel validates it before spawning (compatibility, permissions,
dependencies); when absent the plugin is spawned with no manifest-derived checks.

### Schema

```json
{
  "plugin_id": "stt-whisper",
  "version": "1.2.0",
  "permissions": ["audio_stream", "network"],
  "kernel_compatibility_range": {
    "min": "0.3.0",
    "max": "1.0.0"
  },
  "binary": "stt-whisper",
  "events": ["system.ready"],
  "actions": [
    {
      "name": "transcribe_audio",
      "permission": "audio",
      "input": { "type": "object", "properties": { "format": { "type": "string" } } },
      "output": { "type": "object", "properties": { "text": { "type": "string" } } }
    }
  ],
  "config_schema": {
    "type": "object",
    "properties": {
      "WHISPER_MODEL": { "type": "string", "default": "base" }
    }
  },
  "files": ["stt-whisper", "plugin.json", "models/ggml-tiny.bin"],
  "requires": ["audio-router"]
}
```

### Field Rules

| Field | Type | Required | Rules |
|-------|------|----------|-------|
| `plugin_id` | string | Yes | Must match the registry `slug` for marketplace plugins. Free-form for local-only plugins. |
| `version` | string | Yes | Semver of the installed plugin. |
| `permissions` | string[] | Yes | Exhaustive list. Kernel denies any permission the plugin tries to use that is not listed here. |
| `kernel_compatibility_range` | object | Yes | Must contain `min` and `max` fields (both strings). |
| `kernel_compatibility_range.min` | string | Yes | Semver lower bound (inclusive). |
| `kernel_compatibility_range.max` | string | Yes | Semver upper bound (inclusive) or `"*"`. |
| `binary` | string | Yes | Relative path to the executable within the plugin directory. |
| `events` | string[] | No | Event types the plugin subscribes to (auto-subscribed at load). Empty array if none. |
| `actions` | array of objects | No | Action identifiers the plugin exposes. **Manifest v2**: each element is an object `{ "name", "permission"?, "input"?, "output"? }` (see below). The **legacy string form** (`["transcribe_audio"]`) is still accepted by the kernel. Empty array if none. |
| `actions[].name` | string | Yes | Action identifier. Must be unique within the manifest. |
| `actions[].permission` | string | No | Lowercase permission name (`storage`) or proto name (`PERMISSION_STORAGE`). The permission a **caller** must hold to invoke the action (data-driven anti-laundering, T-19: the provider's own grant authorizes it to *perform* the action, not arbitrary callers to *invoke* it). Absent/empty = unrestricted (declared action is authorization enough). |
| `actions[].input` | object | No | JSON Schema (draft-07 subset) describing the action's `params_json` input. Informational — the kernel does not validate it. |
| `actions[].output` | object | No | JSON Schema (draft-07 subset) describing the action's `data_json` output. Informational — the kernel does not validate it. |
| `config_schema` | object | No | JSON Schema (draft-07 subset) describing the plugin's configuration; properties keyed by env-var name. The kernel does **not** validate it (dumb core). |
| `files` | string[] | No | Explicit list of archive files extracted into the plugin dir (root-relative names as stored in the archive). Doubles as the installer **extraction allowlist** — when present and non-empty, only these entries are extracted, and an allowlisted name missing from the archive aborts the install. Empty/absent = legacy extract-everything. |
| `requires` | string[] | No | Plugin IDs that must be declared in config and are loaded first. Missing deps or dependency cycles refuse the plugin. |

Delivery data (`archive_url`, `sha256`, `signature`) lives in the **registry**, never in the
manifest — the manifest describes what the plugin *is*, the registry describes how to *get* it.

> **Manifest v2 note.** `actions` objects, `config_schema`, and `files` are Manifest v2 fields.
> The kernel parses them leniently and keeps accepting the legacy string-form `actions` array and
> manifests without `files` (which extract the whole archive, as before).

---

## Kernel Load-Time Enforcement

Before spawning any plugin process, the kernel MUST perform these checks in order. On any
failure, the plugin is **skipped** (not loaded) and an error is logged. The kernel continues
loading remaining plugins — one bad plugin does not crash the kernel.

### Step 1 — Read manifest

Read `plugin.json` from the plugin binary's directory. A **missing** file skips all
manifest-derived checks (Steps 2–4) — the plugin is spawned unvalidated (local/dev plugins).
A **present but unparseable** file refuses the plugin:

```
Refusing to load plugin '<dir>': Invalid plugin.json: <parse error>
```

### Step 2 — Kernel version compatibility

Parse `kernel_compatibility_range.min` and `kernel_compatibility_range.max`. Compare against the
running kernel version (from `env!("CARGO_PKG_VERSION")`).

If `running_kernel < min`:

```
Refusing to load plugin '<plugin_id>': requires kernel >= <min>, <= <max>, running <current>
```

If `running_kernel > max` (and `max != "*"`):

```
Refusing to load plugin '<plugin_id>': requires kernel >= <min>, <= <max>, running <current>
```

### Step 3 — Permission validation

Each entry in `permissions` must map to a known `PermissionType` in the proto enum.

Unknown permission:

```
Refusing to load plugin '<plugin_id>': unknown permission '<perm>'
```

### Step 3b — Per-action permission validation (Manifest v2)

Every `actions[].permission` (v2 object-form actions) must also resolve to a known
`PermissionType` (both the lowercase and `PERMISSION_`-prefixed proto forms are accepted).
This is **fail-closed**: an unknown per-action permission refuses the whole plugin, so a typo
can't silently downgrade the action to unrestricted:

```
Refusing to load plugin '<plugin_id>': unknown action permission '<perm>' for action '<action>'
```

### Step 4 — Config-granted permission cross-check

Each declared permission must be granted for this plugin in `config.yaml`. Permission declared
but not granted:

```
Plugin '<plugin_id>' requests permission '<perm>' which is not granted in config
```

### Action-routing authorization (data-driven anti-laundering)

At action-routing time, the kernel resolves the permission a caller must hold to invoke an
action from the **provider-declared per-action permission** (`actions[].permission`, Manifest v2).
When a requirement exists, **both** the provider and the requester must hold the permission —
checking the provider alone would let an unprivileged plugin launder the action through a
permitted provider (T-19).

Actions without a v2 `action_requirement` are unrestricted by default. The legacy hardcoded
fallback (`required_permission_for_action`) has been removed (F5). Plugins that previously
relied on the implicit `http_request → network` mapping MUST declare `action_requirement`
in their v2 manifest.

---

## vyn install Compatibility Check

`vyn install <slug>` checks registry `min_kernel_version` / `max_kernel_version` **before**
downloading the archive (Step 2 of the install pipeline). Error message format:

```
Plugin '<slug>' requires Vynkor kernel >= <min>, <= <max>. You are running <current>. Upgrade Vynkor first.
```

After extraction, `vyn install` re-validates using the local `plugin.json`
`kernel_compatibility_range` as a defense-in-depth check (Step 7 of the install pipeline).

---

## Permissions Reference

String names used in `registry.json` `permissions` and `plugin.json` `permissions` map to
`PermissionType` proto enum values. Both the lowercase form (`storage`) and the
`PERMISSION_`-prefixed proto name (`PERMISSION_STORAGE`) are accepted; the allowed set is
derived from the enum, so a permission added to the proto is installable without a kernel change:

| String name | Proto value | Meaning |
|-------------|-------------|---------|
| `network` | `PERMISSION_NETWORK` | Outbound HTTP requests |
| `files_read` | `PERMISSION_FILES_READ` | Read local files |
| `files_write` | `PERMISSION_FILES_WRITE` | Write/delete local files |
| `system` | `PERMISSION_SYSTEM` | System metrics (CPU, RAM, disk) |
| `audio` | `PERMISSION_AUDIO` | play_audio / record_audio via ActionRequest |
| `notify` | `PERMISSION_NOTIFY` | Send notifications |
| `scheduler` | `PERMISSION_SCHEDULER` | Timers and alarms |
| `browser` | `PERMISSION_BROWSER` | Browser control |
| `ipc_send` | `PERMISSION_IPC_SEND` | Unicast/broadcast to other plugins (also needs `ipc_targets`) |
| `audio_stream` | `PERMISSION_AUDIO_STREAM` | Peer-to-peer raw audio via FLAG_RAW_BINARY |
| `kernel_admin` | `PERMISSION_KERNEL_ADMIN` | Admin `KernelCommand`s (e.g. `reload_config`); `health_check` is exempt |
| `event_publish` | `PERMISSION_EVENT_PUBLISH` | Publish events to the kernel event bus |
| `storage` | `PERMISSION_STORAGE` | Per-caller KV/SQL storage (database plugin) |

This list is normative and derived from the `PermissionType` enum in
`../vynkor-wire/proto/vynkor_protocol.proto` (see `known_permissions()` in `src/marketplace/installer.rs`).
