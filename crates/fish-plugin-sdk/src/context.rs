use std::sync::Arc;

use fish_adapter::adapter::BaseAdapter;
use fish_core::ctx::Ctx;
use fish_core::error::{AppError, Result};
use fish_core::event::{MessageEvent, SystemEvent};
use fish_core::message::MessageSegment;
use fish_core::telemetry::Telemetry;

/// Context passed to plugin handlers.
///
/// Can represent either a message handler context (`#[command]`, `#[message]`)
/// or a system event handler context (`#[event]`). Some methods are only
/// available in one variant and return `Err` in the other.
pub struct Context {
    pub(crate) inner: ContextInner,
}

pub(crate) enum ContextInner {
    Message {
        event: MessageEvent,
        adapter: Arc<dyn BaseAdapter>,
        app_ctx: Arc<Ctx>,
        telemetry: Arc<Telemetry>,
    },
    System {
        event: Arc<SystemEvent>,
        adapter: Arc<dyn BaseAdapter>,
        app_ctx: Arc<Ctx>,
        telemetry: Arc<Telemetry>,
    },
}

impl Context {
    /// Create from a `HandlerContext` (message handlers).
    pub(crate) fn new(
        event: MessageEvent,
        adapter: Arc<dyn BaseAdapter>,
        app_ctx: Arc<Ctx>,
        telemetry: Arc<Telemetry>,
    ) -> Self {
        Self {
            inner: ContextInner::Message {
                event,
                adapter,
                app_ctx,
                telemetry,
            },
        }
    }

    /// Create from a system event (event handlers).
    pub(crate) fn new_from_event(
        event: Arc<SystemEvent>,
        adapter: Arc<dyn BaseAdapter>,
        app_ctx: Arc<Ctx>,
    ) -> Self {
        Self {
            inner: ContextInner::System {
                event,
                adapter,
                app_ctx,
                telemetry: Arc::new(Telemetry::new()),
            },
        }
    }

    // -- Message-only methods --

    /// Reply with a text message. Available only in message handler context.
    pub async fn reply(&self, text: impl Into<String>) -> Result<()> {
        match &self.inner {
            ContextInner::Message { event, .. } => {
                event.reply(MessageSegment::text(text.into())).await;
                Ok(())
            }
            ContextInner::System { .. } => Err(AppError::internal(
                "reply is not available in event handler context",
            )),
        }
    }

    /// The sender's user ID. Available only in message handler context.
    pub fn sender_id(&self) -> Result<&str> {
        match &self.inner {
            ContextInner::Message { event, .. } => Ok(&event.sender_id),
            ContextInner::System { .. } => Err(AppError::internal(
                "sender_id is not available in event handler context",
            )),
        }
    }

    /// The conversation / channel ID. Available only in message handler context.
    pub fn cid(&self) -> Result<&str> {
        match &self.inner {
            ContextInner::Message { event, .. } => Ok(&event.cid),
            ContextInner::System { .. } => Err(AppError::internal(
                "cid is not available in event handler context",
            )),
        }
    }

    /// The plain text content of the message. Available only in message handler context.
    pub fn text(&self) -> Result<String> {
        match &self.inner {
            ContextInner::Message { event, .. } => Ok(event.plain_text()),
            ContextInner::System { .. } => Err(AppError::internal(
                "text is not available in event handler context",
            )),
        }
    }

    /// The raw message event. Available only in message handler context.
    pub fn event(&self) -> Result<&MessageEvent> {
        match &self.inner {
            ContextInner::Message { event, .. } => Ok(event),
            ContextInner::System { .. } => Err(AppError::internal(
                "event is only available in message handler context",
            )),
        }
    }

    // -- System-only methods --

    /// The system event type (e.g. "order_create"). Available only in event handler context.
    pub fn event_type(&self) -> Result<&str> {
        match &self.inner {
            ContextInner::Message { .. } => Err(AppError::internal(
                "event_type is only available in event handler context",
            )),
            ContextInner::System { event, .. } => Ok(&event.event_type),
        }
    }

    /// The raw system event payload. Available only in event handler context.
    pub fn payload(&self) -> Result<&serde_json::Value> {
        match &self.inner {
            ContextInner::Message { .. } => Err(AppError::internal(
                "payload is only available in event handler context",
            )),
            ContextInner::System { event, .. } => Ok(&event.payload),
        }
    }

    // -- Common methods --

    /// Access the adapter for sending messages.
    pub fn adapter(&self) -> &Arc<dyn BaseAdapter> {
        match &self.inner {
            ContextInner::Message { adapter, .. } => adapter,
            ContextInner::System { adapter, .. } => adapter,
        }
    }

    /// Access the shared application context.
    pub fn app_ctx(&self) -> &Arc<Ctx> {
        match &self.inner {
            ContextInner::Message { app_ctx, .. } => app_ctx,
            ContextInner::System { app_ctx, .. } => app_ctx,
        }
    }

    /// Access telemetry counters.
    pub fn telemetry(&self) -> &Arc<Telemetry> {
        match &self.inner {
            ContextInner::Message { telemetry, .. } => telemetry,
            ContextInner::System { telemetry, .. } => telemetry,
        }
    }
}
