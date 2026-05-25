use std::sync::Arc;

use fish_core::ctx::Ctx;
use fish_core::error::Result;
use fish_core::event::{MessageEvent, SystemEvent};
use fish_core::message::MessageChain;
use fish_core::telemetry::Telemetry;

use crate::{ActorBusHandle, AppError, BaseAdapter};

/// Plugin-facing message context for `#[message]` handlers.
pub struct MessageContext {
    event: MessageEvent,
    adapter: Arc<dyn BaseAdapter>,
    app_ctx: Arc<Ctx>,
    telemetry: Arc<Telemetry>,
}

impl MessageContext {
    pub fn new(
        event: MessageEvent,
        adapter: Arc<dyn BaseAdapter>,
        app_ctx: Arc<Ctx>,
        telemetry: Arc<Telemetry>,
    ) -> Self {
        Self {
            event,
            adapter,
            app_ctx,
            telemetry,
        }
    }

    pub async fn reply(&self, message: impl Into<MessageChain>) -> Result<()> {
        let message = message.into();
        if let Err(err) = self
            .adapter
            .send(&self.event.sender_id, &message, Some(&self.event.cid))
            .await
        {
            self.telemetry
                .reply_failures
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(err);
        }
        Ok(())
    }

    pub fn sender_id(&self) -> &str {
        &self.event.sender_id
    }

    pub fn cid(&self) -> &str {
        &self.event.cid
    }

    pub fn text(&self) -> String {
        self.event.plain_text()
    }

    pub fn event(&self) -> &MessageEvent {
        &self.event
    }

    pub fn adapter(&self) -> &Arc<dyn BaseAdapter> {
        &self.adapter
    }

    pub fn app_ctx(&self) -> &Arc<Ctx> {
        &self.app_ctx
    }

    pub fn telemetry(&self) -> &Arc<Telemetry> {
        &self.telemetry
    }

    pub fn bus(&self) -> Result<ActorBusHandle> {
        self.app_ctx
            .get::<ActorBusHandle>()
            .map(|handle| (*handle).clone())
            .ok_or_else(|| AppError::internal("actor bus is unavailable"))
    }
}

/// Plugin-facing event context for `#[event]` handlers.
pub struct EventContext {
    event: Arc<SystemEvent>,
    adapter: Arc<dyn BaseAdapter>,
    app_ctx: Arc<Ctx>,
    telemetry: Arc<Telemetry>,
}

impl EventContext {
    pub fn new(
        event: Arc<SystemEvent>,
        adapter: Arc<dyn BaseAdapter>,
        app_ctx: Arc<Ctx>,
        telemetry: Arc<Telemetry>,
    ) -> Self {
        Self {
            event,
            adapter,
            app_ctx,
            telemetry,
        }
    }

    pub fn event_type(&self) -> &str {
        &self.event.event_type
    }

    pub fn payload(&self) -> &serde_json::Value {
        &self.event.payload
    }

    pub fn event(&self) -> &SystemEvent {
        &self.event
    }

    pub fn adapter(&self) -> &Arc<dyn BaseAdapter> {
        &self.adapter
    }

    pub fn app_ctx(&self) -> &Arc<Ctx> {
        &self.app_ctx
    }

    pub fn telemetry(&self) -> &Arc<Telemetry> {
        &self.telemetry
    }

    pub fn bus(&self) -> Result<ActorBusHandle> {
        self.app_ctx
            .get::<ActorBusHandle>()
            .map(|handle| (*handle).clone())
            .ok_or_else(|| AppError::internal("actor bus is unavailable"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::{ActorBusHandle, RuntimeActorBus};
    use async_trait::async_trait;
    use fish_core::AdapterEventSink;

    struct RecordAdapter {
        sent: Arc<tokio::sync::Mutex<Vec<(String, Option<String>, String)>>>,
    }

    #[async_trait]
    impl BaseAdapter for RecordAdapter {
        async fn send(
            &self,
            target_id: &str,
            message: &MessageChain,
            cid: Option<&str>,
        ) -> Result<()> {
            self.sent.lock().await.push((
                target_id.to_string(),
                cid.map(str::to_string),
                message.summary(),
            ));
            Ok(())
        }

        async fn run(&self, _sink: Arc<dyn AdapterEventSink>) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn t3_1_message_context_reply_uses_adapter_send() -> Result<()> {
        let sent = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let adapter: Arc<dyn BaseAdapter> = Arc::new(RecordAdapter {
            sent: Arc::clone(&sent),
        });
        let ctx = MessageContext::new(
            MessageEvent::new(
                "demo-cid".into(),
                "demo-user".into(),
                "Demo User".into(),
                MessageChain::from("/ping"),
                serde_json::json!({}),
            ),
            adapter,
            Arc::new(Ctx::new()),
            Arc::new(Telemetry::new()),
        );

        ctx.reply("pong").await?;

        let guard = sent.lock().await;
        assert_eq!(guard.len(), 1);
        assert_eq!(guard[0].0, "demo-user");
        assert_eq!(guard[0].1.as_deref(), Some("demo-cid"));
        assert_eq!(guard[0].2, "pong");
        Ok(())
    }

    #[tokio::test]
    async fn t3_2_message_context_exposes_runtime_bus() -> Result<()> {
        let app_ctx = Arc::new(Ctx::new());
        app_ctx.insert(ActorBusHandle::new(Arc::new(RuntimeActorBus::default())));

        let ctx = MessageContext::new(
            MessageEvent::new(
                "demo-cid".into(),
                "demo-user".into(),
                "Demo User".into(),
                MessageChain::from("/ping"),
                serde_json::json!({}),
            ),
            Arc::new(RecordAdapter {
                sent: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            }),
            app_ctx,
            Arc::new(Telemetry::new()),
        );

        assert!(ctx.bus().is_ok());
        Ok(())
    }
}
