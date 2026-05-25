mod app;

use fish_runtime::Result;

#[tokio::main]
async fn main() -> Result<()> {
    app::run().await
}
