use fish_rt_adapter::plugin;
use fish_rt_adapter::prelude::*;

pub struct EchoPlugin;

#[plugin]
impl EchoPlugin {
    #[message("/ping")]
    async fn ping(&self, ctx: MessageContext) -> Result<()> {
        ctx.reply("pong").await?;
        Ok(())
    }

    #[message(keyword = "你好")]
    async fn greet(&self, ctx: MessageContext) -> Result<()> {
        ctx.reply(format!("收到: {}", ctx.text())).await?;
        Ok(())
    }
}
