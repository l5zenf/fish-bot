use std::sync::Arc;

use fish_rt_adapter::FishWebSocketAdapter;
use fish_runtime::prelude::*;
use fish_runtime::{BaseAdapter, RuntimeHost, Telemetry};

use super::plugin::EchoPlugin;

pub async fn run() -> Result<()> {
    init_tracing();

    let bootstrap = FishAppBootstrap::new();
    bootstrap.run().await
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .compact()
        .try_init();
}

struct FishAppBootstrap {
    adapter: Arc<dyn BaseAdapter>,
}

impl FishAppBootstrap {
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
