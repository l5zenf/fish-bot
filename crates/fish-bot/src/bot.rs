use std::sync::Arc;

use kameo::prelude::*;
use kameo::message::{Context, Message};

use fish_adapter::adapter::BaseAdapter;
use fish_core::ctx::Ctx;
use fish_core::event::MessageEvent;
use fish_core::message::{MessageChain, MessageSegment};
use fish_plugin::plugin::actor::{HandleEvent, PluginActor};

/// Bot actor — receives MessageEvents and fans out to all PluginActors.
/// Matching Python bot.py Bot class, powered by kameo actor runtime.
#[derive(Actor)]
pub struct Bot {
    adapter: Arc<dyn BaseAdapter>,
    plugin_refs: Vec<ActorRef<PluginActor>>,
    ctx: Arc<Ctx>,
}

impl Bot {
    pub fn new(
        adapter: Arc<dyn BaseAdapter>,
        plugin_refs: Vec<ActorRef<PluginActor>>,
        ctx: Arc<Ctx>,
    ) -> Self {
        Self {
            adapter,
            plugin_refs,
            ctx,
        }
    }
}

// ---- Messages ----

/// Dispatch a message event — sent by the adapter when a new message arrives.
pub struct DispatchEvent {
    pub event: MessageEvent,
}

impl Message<DispatchEvent> for Bot {
    type Reply = ();

    async fn handle(
        &mut self,
        msg: DispatchEvent,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        // Create reply callback bound to this event's sender
        let reply_adapter = Arc::clone(&self.adapter);
        let reply_cid = msg.event.cid.clone();
        let reply_target = msg.event.sender_id.clone();

        let mut event = msg.event;
        event.set_callback(move |reply_msg: MessageSegment| {
            let adapter = Arc::clone(&reply_adapter);
            let target = reply_target.clone();
            let cid = reply_cid.clone();
            Box::pin(async move {
                let chain = MessageChain::from(reply_msg);
                if let Err(e) = adapter.send(&target, &chain, Some(&cid)).await {
                    tracing::error!("Failed to send reply: {}", e);
                }
            })
        });

        let adapter = Arc::clone(&self.adapter);
        let ctx = Arc::clone(&self.ctx);

        // Fan out to all plugin actors — each checks its own rules in isolation
        for plugin_ref in &self.plugin_refs {
            let _ = plugin_ref
                .tell(HandleEvent {
                    event: event.clone(),
                    adapter: Arc::clone(&adapter),
                    ctx: Arc::clone(&ctx),
                })
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use fish_adapter::adapter::BaseAdapter;
    use fish_core::error::Result;
    use fish_core::rule::{is_fullmatch};
    use fish_plugin::plugin::{MessageHandler, Plugin, PluginMetadata};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use kameo::actor::Spawn;

    struct MockAdapter;
    #[async_trait]
    impl BaseAdapter for MockAdapter {
        fn set_callback(&self, _: Box<dyn Fn(MessageEvent) + Send + Sync>) {}
        async fn send(&self, _: &str, _: &MessageChain, _: Option<&str>) -> Result<()> { Ok(()) }
        async fn run(&self) -> Result<()> { Ok(()) }
    }

    struct CounterPlugin(Arc<AtomicUsize>);
    impl Plugin for CounterPlugin {
        fn metadata(&self) -> PluginMetadata {
            PluginMetadata { id: "counter".into(), name: "".into(), description: "".into(), ..Default::default() }
        }
        fn message_handlers(&self) -> Vec<MessageHandler> {
            let count = Arc::clone(&self.0);
            vec![MessageHandler {
                func: Arc::new(move |_, _, _| {
                    let c = Arc::clone(&count);
                    Box::pin(async move { c.fetch_add(1, Ordering::SeqCst); })
                }),
                rule: Some(is_fullmatch(["/ping"])),
            }]
        }
    }

    fn make_event(text: &str) -> MessageEvent {
        MessageEvent::new(
            "cid".into(), "sender".into(), "name".into(),
            MessageChain::from(text),
            Default::default(),
        )
    }

    #[test]
    fn t4_2_bot_new() {
        let adapter: Arc<dyn BaseAdapter> = Arc::new(MockAdapter);
        let ctx = Arc::new(Ctx::new());
        let bot = Bot::new(adapter, vec![], ctx);
        assert!(bot.plugin_refs.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t4_3_dispatch_sets_callback() {
        let recorded = Arc::new(tokio::sync::Mutex::new(String::new()));
        let recorded_clone = Arc::clone(&recorded);

        struct RecordAdapter(Arc<tokio::sync::Mutex<String>>);
        #[async_trait]
        impl BaseAdapter for RecordAdapter {
            fn set_callback(&self, _: Box<dyn Fn(MessageEvent) + Send + Sync>) {}
            async fn send(&self, _target: &str, msg: &MessageChain, _cid: Option<&str>) -> Result<()> {
                let mut guard = self.0.lock().await;
                *guard = msg.summary();
                Ok(())
            }
            async fn run(&self) -> Result<()> { Ok(()) }
        }

        let adapter: Arc<dyn BaseAdapter> = Arc::new(RecordAdapter(recorded_clone));
        let ctx = Arc::new(Ctx::new());

        // Plugin that echoes back via reply — this triggers Bot's reply callback → adapter.send
        struct EchoPlugin;
        impl Plugin for EchoPlugin {
            fn metadata(&self) -> PluginMetadata { PluginMetadata { id: "echo".into(), name: "".into(), description: "".into(), ..Default::default() } }
            fn message_handlers(&self) -> Vec<MessageHandler> {
                vec![MessageHandler {
                    func: Arc::new(|event, _, _| {
                        let content = event.plain_text();
                        Box::pin(async move {
                            let _ = event.reply(MessageSegment::text(content)).await;
                        })
                    }),
                    rule: None,
                }]
            }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(EchoPlugin);
        let plugin_ref = PluginActor::spawn(PluginActor::new(plugin));
        let bot_ref = Bot::spawn(Bot::new(adapter, vec![plugin_ref], ctx));

        let event = make_event("/ping");

        let _ = bot_ref.tell(DispatchEvent { event }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Bot should have set the reply callback → adapter.send was called with the echoed message
        let guard = recorded.lock().await;
        assert_eq!(*guard, "/ping", "adapter.send should have been called with the echoed message");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t4_4_dispatch_fan_out() {
        let adapter: Arc<dyn BaseAdapter> = Arc::new(MockAdapter);
        let ctx = Arc::new(Ctx::new());

        let count1 = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::new(AtomicUsize::new(0));

        let plugin1: Arc<dyn Plugin> = Arc::new(CounterPlugin(Arc::clone(&count1)));
        let plugin2: Arc<dyn Plugin> = Arc::new(CounterPlugin(Arc::clone(&count2)));

        let pref1 = PluginActor::spawn(PluginActor::new(plugin1));
        let pref2 = PluginActor::spawn(PluginActor::new(plugin2));

        let bot_ref = Bot::spawn(Bot::new(adapter, vec![pref1, pref2], ctx));

        let mut event = make_event("/ping");
        event.set_callback(|_| Box::pin(async {}));

        let _ = bot_ref.tell(DispatchEvent { event }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        assert_eq!(count1.load(Ordering::SeqCst), 1);
        assert_eq!(count2.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t4_5_dispatch_empty_plugin_refs() {
        let adapter: Arc<dyn BaseAdapter> = Arc::new(MockAdapter);
        let ctx = Arc::new(Ctx::new());
        let bot_ref = Bot::spawn(Bot::new(adapter, vec![], ctx));

        let mut event = make_event("/ping");
        event.set_callback(|_| Box::pin(async {}));

        // Should not panic with no plugins
        let _ = bot_ref.tell(DispatchEvent { event }).await;
    }
}
