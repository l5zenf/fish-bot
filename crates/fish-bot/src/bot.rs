use std::sync::Arc;

use kameo::prelude::*;
use kameo::message::{Context, Message};

use fish_adapter::adapter::BaseAdapter;
use fish_core::ctx::Ctx;
use fish_core::event::MessageEvent;
use fish_core::message::{MessageChain, MessageSegment};
use fish_plugin::plugin::actor::{HandleEvent, PluginActor};

/// Bot actor — receives MessageEvents and fans out to all PluginActors.
#[derive(Actor)]
pub struct Bot {
    adapter: Arc<dyn BaseAdapter>,
    plugin_refs: Vec<(ActorRef<PluginActor>, Arc<dyn fish_plugin::plugin::Plugin>)>,
    ctx: Arc<Ctx>,
}

impl Bot {
    pub fn new(
        adapter: Arc<dyn BaseAdapter>,
        plugin_refs: Vec<(ActorRef<PluginActor>, Arc<dyn fish_plugin::plugin::Plugin>)>,
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

        // Fan out to plugin actors — pre-filter by Plugin::supports() to skip
        // plugins whose rules can't match, avoiding unnecessary actor dispatch.
        for (plugin_ref, plugin) in &self.plugin_refs {
            if !plugin.supports(&event) {
                continue;
            }
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
    use fish_core::error::{AppError, Result};
    use fish_core::rule::{is_fullmatch};
    use fish_plugin::plugin::{MessageHandler, Plugin, PluginMetadata};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use kameo::actor::Spawn;

    struct MockAdapter;
    #[async_trait]
    impl BaseAdapter for MockAdapter {
        fn set_callback(&self, _: Box<dyn Fn(MessageEvent) + Send + Sync>) {}
        async fn send(&self, _: &str, _: &MessageChain, _: Option<&str>) -> Result<()> { Ok(()) }
        async fn run(&self) -> Result<()> { Ok(()) }
    }

    struct CounterPlugin {
        meta: PluginMetadata,
        handlers: Vec<MessageHandler>,
    }
    impl Plugin for CounterPlugin {
        fn metadata(&self) -> &PluginMetadata { &self.meta }
        fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
    }

    fn make_counter_plugin(count: Arc<AtomicUsize>) -> CounterPlugin {
        CounterPlugin {
            meta: PluginMetadata { id: "counter".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("counter", Some(is_fullmatch(["/ping"])), Arc::new(move |_, _, _| {
                let c = Arc::clone(&count);
                Box::pin(async move { c.fetch_add(1, Ordering::SeqCst); Ok(()) })
            }))],
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

        struct EchoPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for EchoPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(EchoPlugin {
            meta: PluginMetadata { id: "echo".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("echo", None, Arc::new(|event, _, _| {
                let content = event.plain_text();
                Box::pin(async move {
                    let _ = event.reply(MessageSegment::text(content)).await;
                    Ok(())
                })
            }))],
        });
        let plugin_for_bot = Arc::clone(&plugin);
        let plugin_ref = PluginActor::spawn(PluginActor::new(plugin));
        let bot_ref = Bot::spawn(Bot::new(adapter, vec![(plugin_ref, plugin_for_bot)], ctx));

        let event = make_event("/ping");

        let _ = bot_ref.tell(DispatchEvent { event }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let guard = recorded.lock().await;
        assert_eq!(*guard, "/ping", "adapter.send should have been called with the echoed message");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t4_4_dispatch_fan_out() {
        let adapter: Arc<dyn BaseAdapter> = Arc::new(MockAdapter);
        let ctx = Arc::new(Ctx::new());

        let count1 = Arc::new(AtomicUsize::new(0));
        let count2 = Arc::new(AtomicUsize::new(0));

        let plugin1: Arc<dyn Plugin> = Arc::new(make_counter_plugin(Arc::clone(&count1)));
        let plugin2: Arc<dyn Plugin> = Arc::new(make_counter_plugin(Arc::clone(&count2)));
        let p1 = Arc::clone(&plugin1);
        let p2 = Arc::clone(&plugin2);
        let pref1 = PluginActor::spawn(PluginActor::new(plugin1));
        let pref2 = PluginActor::spawn(PluginActor::new(plugin2));

        let bot_ref = Bot::spawn(Bot::new(adapter, vec![(pref1, p1), (pref2, p2)], ctx));

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

        let _ = bot_ref.tell(DispatchEvent { event }).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t4_7_dispatch_reply_send_error() -> anyhow::Result<()> {
        struct ErrorAdapter;
        #[async_trait]
        impl BaseAdapter for ErrorAdapter {
            fn set_callback(&self, _: Box<dyn Fn(MessageEvent) + Send + Sync>) {}
            async fn send(&self, _: &str, _: &MessageChain, _: Option<&str>) -> Result<()> {
                Err(AppError::http("simulated error"))
            }
            async fn run(&self) -> Result<()> { Ok(()) }
        }

        let adapter: Arc<dyn BaseAdapter> = Arc::new(ErrorAdapter);
        let ctx = Arc::new(Ctx::new());

        struct ReplyPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for ReplyPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(ReplyPlugin {
            meta: PluginMetadata { id: "reply".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("reply", None, Arc::new(|event, _, _| {
                Box::pin(async move {
                    let _ = event.reply(MessageSegment::text("reply")).await;
                    Ok(())
                })
            }))],
        });
        let plugin_for_bot = Arc::clone(&plugin);
        let plugin_ref = PluginActor::spawn(PluginActor::new(plugin));
        let bot_ref = Bot::spawn(Bot::new(adapter, vec![(plugin_ref, plugin_for_bot)], ctx));

        let mut event = make_event("/test");
        event.set_callback(|_| Box::pin(async {}));

        let _ = bot_ref.tell(DispatchEvent { event }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t4_8_dispatch_rule_not_matching() -> anyhow::Result<()> {
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = Arc::clone(&called);

        struct SelectivePlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for SelectivePlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let adapter: Arc<dyn BaseAdapter> = Arc::new(MockAdapter);
        let ctx = Arc::new(Ctx::new());

        let plugin: Arc<dyn Plugin> = Arc::new(SelectivePlugin {
            meta: PluginMetadata { id: "selective".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("selective", Some(is_fullmatch(["/run"])), Arc::new(move |_, _, _| {
                let f = Arc::clone(&called_clone);
                Box::pin(async move { f.store(true, Ordering::SeqCst); Ok(()) })
            }))],
        });
        let plugin_for_bot = Arc::clone(&plugin);
        let plugin_ref = PluginActor::spawn(PluginActor::new(plugin));
        let bot_ref = Bot::spawn(Bot::new(adapter, vec![(plugin_ref, plugin_for_bot)], ctx));

        let mut event = make_event("/skip");
        event.set_callback(|_| Box::pin(async {}));

        let _ = bot_ref.tell(DispatchEvent { event }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        assert!(!called.load(Ordering::SeqCst), "handler should not be called when rule doesn't match");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn t4_9_dispatch_reply_multi_segment() -> anyhow::Result<()> {
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

        struct MultiReplyPlugin {
            meta: PluginMetadata,
            handlers: Vec<MessageHandler>,
        }
        impl Plugin for MultiReplyPlugin {
            fn metadata(&self) -> &PluginMetadata { &self.meta }
            fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
        }

        let plugin: Arc<dyn Plugin> = Arc::new(MultiReplyPlugin {
            meta: PluginMetadata { id: "multi_reply".into(), ..Default::default() },
            handlers: vec![MessageHandler::new("multi_reply", None, Arc::new(|event, _, _| {
                Box::pin(async move {
                    let _ = event.reply(MessageSegment::text("multi segment reply")).await;
                    Ok(())
                })
            }))],
        });
        let plugin_for_bot = Arc::clone(&plugin);
        let plugin_ref = PluginActor::spawn(PluginActor::new(plugin));
        let bot_ref = Bot::spawn(Bot::new(adapter, vec![(plugin_ref, plugin_for_bot)], ctx));

        let event = make_event("/test");
        let _ = bot_ref.tell(DispatchEvent { event }).await;
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let guard = recorded.lock().await;
        assert_eq!(*guard, "multi segment reply");
        Ok(())
    }
}
