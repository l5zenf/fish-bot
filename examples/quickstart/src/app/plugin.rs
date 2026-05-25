use fish_runtime::prelude::*;
use fish_runtime::{plugin, plugin_handlers};

#[plugin(id = "echo", name = "Echo Plugin")]
#[derive(Default)]
pub struct EchoPlugin;

#[plugin_handlers]
impl EchoPlugin {
    #[command("/ping")]
    async fn ping(&self, ctx: Context) -> Result<()> {
        ctx.reply("pong").await?;
        Ok(())
    }

    #[message(keyword = "fish")]
    async fn on_keyword(&self, ctx: Context) -> Result<()> {
        ctx.reply(format!("keyword hit: {}", ctx.text()?)).await?;
        Ok(())
    }
}
