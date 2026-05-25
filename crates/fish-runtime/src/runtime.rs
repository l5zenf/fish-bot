use std::time::Duration;

/// Queue strategy when a plugin's handler concurrency limit is reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueStrategy {
    DropNewest,
    DropOldest(usize),
}

impl Default for QueueStrategy {
    fn default() -> Self {
        Self::DropNewest
    }
}

/// Per-plugin runtime configuration.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub concurrency: usize,
    pub timeout: Duration,
    pub queue_strategy: QueueStrategy,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            concurrency: 64,
            timeout: Duration::from_secs(5),
            queue_strategy: QueueStrategy::default(),
        }
    }
}
