use fish_runtime::plugin;
use fish_runtime::prelude::*;

pub struct EchoPlugin;

#[plugin]
impl EchoPlugin {
    #[message("/ping")]
    async fn ping(&self, ctx: MessageContext) -> Result<()> {
        ctx.reply("pong").await?;
        Ok(())
    }

    #[message(keyword = "fish")]
    async fn on_keyword(&self, ctx: MessageContext) -> Result<()> {
        ctx.reply(format!("keyword hit: {}", ctx.text())).await?;
        Ok(())
    }
}
