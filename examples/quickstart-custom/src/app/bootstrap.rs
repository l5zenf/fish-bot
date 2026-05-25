use std::sync::Arc;

use super::plugin::build_plugin;
use fish_rt_adapter::prelude::*;
use fish_rt_adapter::{BaseAdapter, FishWebSocketAdapter, RuntimeHost, Telemetry};

pub async fn run() -> Result<()> {
    init_tracing();

    let bootstrap = QuickstartCustomBootstrap::new();
    bootstrap.run().await
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .compact()
        .try_init();
}

struct QuickstartCustomBootstrap {
    adapter: Arc<dyn BaseAdapter>,
}

impl QuickstartCustomBootstrap {
    fn new() -> Self {
        Self {
            adapter: Arc::new(FishWebSocketAdapter::new()),
        }
    }

    async fn run(self) -> Result<()> {
        let plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(build_plugin())];
        let host = RuntimeHost::new(
            Arc::clone(&self.adapter),
            plugins,
            Arc::new(Ctx::new()),
            Arc::new(Telemetry::new()),
        );
        host.run().await
    }
}
