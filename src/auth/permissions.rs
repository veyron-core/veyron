use crate::plugins::registry::PluginRegistry;
use crate::proto::vynkor::PermissionType;
use crate::utils::errors::VynkorError;
use std::sync::atomic::{AtomicBool, Ordering};

/// F5: hardcoded action→permission fallback REMOVED. The data-driven v2
/// path (`registry.action_requirement`) is the single source of truth.
/// This function exists only to emit a one-time deprecation warning for
/// legacy plugins that relied on the implicit http_request → network map.
/// Easy to delete: remove this function and its single caller in router.rs.
pub fn required_permission_for_action(action: &str) -> Option<PermissionType> {
    static WARNED_HTTP: AtomicBool = AtomicBool::new(false);
    if action == "http_request" && !WARNED_HTTP.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            action = "http_request",
            "hardcoded action→permission fallback removed (F5); \
             the network plugin MUST declare action_requirement in its v2 manifest. \
             see docs/PLUGIN_REGISTRY_SCHEMA.md"
        );
    }
    None
}

/// Normalizes a permission string (lowercase `storage` or proto
/// `PERMISSION_STORAGE`) to its PermissionType, if known. Used for
/// manifest-declared per-action permissions (Manifest v2).
pub fn resolve_permission(s: &str) -> Option<PermissionType> {
    let name = if let Some(rest) = s.strip_prefix("PERMISSION_") {
        format!("PERMISSION_{}", rest.to_ascii_uppercase())
    } else {
        format!("PERMISSION_{}", s.to_ascii_uppercase())
    };
    // `from_str_name` happily resolves the literal "PERMISSION_UNKNOWN" to the
    // UNKNOWN variant (value 0) — that is not a usable requirement, so treat
    // it as unresolvable, matching `known_permissions()` which excludes it.
    let pt = PermissionType::from_str_name(&name)?;
    if pt == PermissionType::PermissionUnknown {
        None
    } else {
        Some(pt)
    }
}

// lowercases and strips the PERMISSION_ prefix so manifests can declare either
// the documented lowercase form (network) or the proto name (PERMISSION_NETWORK).
// pub(crate): config-time comparisons (T-04 clamp, validate_plugin_def) must
// normalize the same way runtime check_permission does (N2).
pub(crate) fn normalize_permission(s: &str) -> String {
    s.strip_prefix("PERMISSION_")
        .unwrap_or(s)
        .to_ascii_lowercase()
}

pub fn check_permission(
    registry: &PluginRegistry,
    plugin_id: &str,
    required: PermissionType,
) -> Result<(), VynkorError> {
    let entry = registry
        .get(plugin_id)
        .ok_or_else(|| VynkorError::PluginNotFound(plugin_id.to_string()))?;

    let required_norm = normalize_permission(required.as_str_name());
    if entry
        .manifest
        .permissions
        .iter()
        .any(|p| normalize_permission(p) == required_norm)
    {
        Ok(())
    } else {
        Err(VynkorError::PermissionDenied(format!(
            "{plugin_id} lacks {}",
            required.as_str_name()
        )))
    }
}

/// Gate peer-to-peer IPC: a plugin may only unicast to another plugin if it
/// declared `PERMISSION_IPC_SEND`. Default-deny — undeclared senders are rejected.
pub fn check_ipc_send(registry: &PluginRegistry, plugin_id: &str) -> Result<(), VynkorError> {
    check_permission(registry, plugin_id, PermissionType::PermissionIpcSend)
}

/// Gate per-target IPC (T-04): sender must list the target in `ipc_targets`.
/// Empty allowlist = deny-all, even with PERMISSION_IPC_SEND. D-03: same-user
/// only — a sender may not talk to another user's plugins.
pub fn check_ipc_target(
    registry: &PluginRegistry,
    sender_id: &str,
    target_id: &str,
) -> Result<(), VynkorError> {
    let entry = registry
        .get(sender_id)
        .ok_or_else(|| VynkorError::PluginNotFound(sender_id.to_string()))?;

    // D-03: same-user only IPC (one comparison). Host plugins all share the
    // "default" user, so single-user deployments are unaffected.
    if let Some(target) = registry.get(target_id) {
        if target.user_id != entry.user_id {
            return Err(VynkorError::PermissionDenied(format!(
                "cross-user IPC denied: {sender_id} (user {}) -> {target_id} (user {})",
                entry.user_id, target.user_id
            )));
        }
    }

    if entry.manifest.ipc_targets.iter().any(|t| t == target_id) {
        Ok(())
    } else {
        Err(VynkorError::PermissionDenied(format!(
            "{sender_id} ipc_targets does not include {target_id}"
        )))
    }
}
