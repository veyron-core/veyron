pub(crate) mod helpers;
pub(crate) mod router;

// Re-export MessageRouter for backward compatibility
pub use router::MessageRouter;
// Re-export helpers used by other modules (e.g. events/bus.rs)
pub(crate) use helpers::kernel_message_id;
