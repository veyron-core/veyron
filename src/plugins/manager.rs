use std::sync::Arc;

use crate::plugins::registry::{PluginEntry, PluginRegistry};
use crate::plugins::supervisor::{PluginConfig, PluginProcess, PluginSupervisor};
use crate::utils::errors::VynkorError;

/// High-level plugin lifecycle API: combines supervisor (process control)
/// and registry (connection state) into one access point.
pub struct PluginManager {
    supervisor: Arc<PluginSupervisor>,
    registry: Arc<PluginRegistry>,
}

impl PluginManager {
    pub fn new(supervisor: Arc<PluginSupervisor>, registry: Arc<PluginRegistry>) -> Self {
        Self {
            supervisor,
            registry,
        }
    }

    pub fn list(&self) -> Vec<Arc<PluginEntry>> {
        self.registry.list()
    }

    pub fn registry(&self) -> &Arc<PluginRegistry> {
        &self.registry
    }

    pub fn get(&self, plugin_id: &str) -> Option<Arc<PluginEntry>> {
        self.registry.get(plugin_id)
    }

    pub async fn start(&self, config: PluginConfig) -> Result<PluginProcess, VynkorError> {
        self.supervisor.spawn_plugin(config).await
    }

    /// Stops the supervised OS process and removes the IPC registration.
    /// Returns the supervisor error if the process couldn't be stopped, but
    /// always unregisters the plugin from the registry regardless.
    pub async fn stop(&self, plugin_id: &str) -> Result<(), VynkorError> {
        let result = self.supervisor.stop_plugin(plugin_id).await;
        self.registry.unregister(plugin_id);
        result
    }

    pub async fn restart(&self, plugin_id: &str) -> Result<(), VynkorError> {
        self.supervisor.restart_plugin(plugin_id).await
    }

    pub fn is_supervised(&self, plugin_id: &str) -> bool {
        self.supervisor.is_running(plugin_id)
    }

    pub fn is_connected(&self, plugin_id: &str) -> bool {
        self.registry.get(plugin_id).is_some()
    }

    pub async fn logs(&self, plugin_id: &str, n: usize) -> Vec<String> {
        self.supervisor.get_logs(plugin_id, n).await
    }
}
