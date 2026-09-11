// F2 (DC-2): device-surface interpretation, moved out of the core registry.
// The registry stores raw wire DeviceOs/DeviceState values and never
// interprets them; the friendly lowercase strings are a consumer-facing
// concern, so the mapping lives here in the API layer. The observability
// surfaces that render these (REST /devices, the list_devices kernel command,
// lifecycle event payloads) all call in from here.
use crate::proto::vynkor::{DeviceOs, DeviceState};

// D-04: display strings for wire DeviceOs values; unknown values read back as
// "unspecified" so the discovery surface never panics
pub fn device_os_str(os: i32) -> &'static str {
    match DeviceOs::try_from(os) {
        Ok(DeviceOs::Linux) => "linux",
        Ok(DeviceOs::Macos) => "macos",
        Ok(DeviceOs::Windows) => "windows",
        Ok(DeviceOs::Android) => "android",
        Ok(DeviceOs::Ios) => "ios",
        Ok(DeviceOs::Freebsd) => "freebsd",
        _ => "unspecified",
    }
}

pub fn device_state_str(state: i32) -> &'static str {
    match DeviceState::try_from(state) {
        Ok(DeviceState::Online) => "online",
        Ok(DeviceState::Offline) => "offline",
        // E-01 (v1.7): revoked devices surface distinctly, never as "online"
        Ok(DeviceState::Revoked) => "revoked",
        _ => "unspecified",
    }
}
