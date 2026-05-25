use fish_rt_adapter::ActorPluginBuilder;
use fish_rt_adapter::prelude::*;
use kameo::Actor;
use kameo::message::{Context, Message};
use tracing::info;

#[derive(Actor)]
pub(crate) struct CounterActor {
    seen: u64,
}

pub fn build_plugin() -> ActorPluginBuilder<CounterActor> {
    ActorPluginBuilder::new(|| CounterActor { seen: 0 })
        .id("quickstart_custom_actor")
        .name("QuickstartCustomActor")
        .bounded_mailbox(64)
        .on_message("/ping", Ping)
        .on_keyword("runtime", KeywordHit)
}

struct Ping(MessageContext);
struct KeywordHit(MessageContext);

impl Message<Ping> for CounterActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: Ping, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        info!("receive message");
        self.seen += 1;
        msg.0.reply(format!("actor pong #{}", self.seen)).await
    }
}

impl Message<KeywordHit> for CounterActor {
    type Reply = Result<()>;

    async fn handle(
        &mut self,
        msg: KeywordHit,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.seen += 1;
        msg.0
            .reply(format!("actor keyword #{}: {}", self.seen, msg.0.text()))
            .await
    }
}
