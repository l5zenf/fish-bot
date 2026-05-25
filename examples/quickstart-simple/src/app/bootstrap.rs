use std::sync::Arc;

use super::plugin::EchoPlugin;
use fish_rt_adapter::prelude::*;
use fish_rt_adapter::{BaseAdapter, FishWebSocketAdapter, RuntimeHost, Telemetry};

pub async fn run() -> Result<()> {
    init_tracing();

    let bootstrap = QuickstartSimpleBootstrap::new();
    bootstrap.run().await
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .compact()
        .try_init();
}

struct QuickstartSimpleBootstrap {
    adapter: Arc<dyn BaseAdapter>,
}

impl QuickstartSimpleBootstrap {
    fn new() -> Self {
        Self {
            adapter: Arc::new(FishWebSocketAdapter::new()),
        }
    }

    async fn run(self) -> Result<()> {
        let plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(EchoPlugin)];
        let host = RuntimeHost::new(
            Arc::clone(&self.adapter),
            plugins,
            Arc::new(Ctx::new()),
            Arc::new(Telemetry::new()),
        );
        host.run().await
    }
}
