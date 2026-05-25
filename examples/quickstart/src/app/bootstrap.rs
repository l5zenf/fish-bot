use std::sync::Arc;

use fish_core::BaseAdapter;
use fish_core::error::Result;
use fish_core::telemetry::Telemetry;
use fish_runtime::RuntimeHost;
use fish_runtime::prelude::*;

use super::local_adapter::LocalAdapter;
use super::plugin::EchoPlugin;

pub async fn run() -> Result<()> {
    init_tracing();

    let bootstrap = QuickstartBootstrap::new();
    bootstrap.run_demo().await
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .compact()
        .try_init();
}

struct QuickstartBootstrap {
    adapter: Arc<dyn BaseAdapter>,
}

impl QuickstartBootstrap {
    fn new() -> Self {
        Self {
            adapter: Arc::new(LocalAdapter),
        }
    }

    async fn run_demo(self) -> Result<()> {
        let plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(EchoPlugin)];
        let host = RuntimeHost::new(
            Arc::clone(&self.adapter),
            plugins,
            Arc::new(Ctx::new()),
            Arc::new(Telemetry::new()),
        );
        host.run().await?;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        Ok(())
    }
}
