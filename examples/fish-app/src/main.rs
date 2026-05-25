mod app;

use fish_rt_adapter::Result;

#[tokio::main]
async fn main() -> Result<()> {
    app::run().await
}
