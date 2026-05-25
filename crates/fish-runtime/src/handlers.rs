use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use fish_core::ctx::Ctx;
use fish_core::error::{AppError, Result};
use fish_core::event::{MessageEvent, SystemEvent};
use fish_core::message::MessageChain;
use fish_core::rule::{Rule, is_fullmatch, is_keywords, is_regex, is_startswith};
use fish_core::telemetry::Telemetry;

use crate::BaseAdapter;
use crate::plugin::PluginState;

/// Route hint for runtime-level routing.
/// Allows `RuntimeHost` to pre-filter messages before dispatching to `PluginActor`.
#[derive(Debug, Clone)]
pub enum RouteHint {
    Exact(Vec<String>),
    Prefix(Vec<String>),
    Keyword(Vec<String>),
    Regex,
    Fallback,
}

/// Context passed to every message handler execution.
#[doc(hidden)]
pub struct HandlerContext {
    pub event: MessageEvent,
    pub adapter: Arc<dyn BaseAdapter>,
    pub app_ctx: Arc<Ctx>,
    pub telemetry: Arc<Telemetry>,
    plugin_state: Option<PluginState>,
}

#[doc(hidden)]
impl HandlerContext {
    pub(crate) fn __new(
        event: MessageEvent,
        adapter: Arc<dyn BaseAdapter>,
        app_ctx: Arc<Ctx>,
        telemetry: Arc<Telemetry>,
        plugin_state: Option<PluginState>,
    ) -> Self {
        Self {
            event,
            adapter,
            app_ctx,
            telemetry,
            plugin_state,
        }
    }

    pub fn state<T: Send + Sync + 'static>(&self) -> Result<Arc<tokio::sync::RwLock<T>>> {
        self.plugin_state
            .clone()
            .and_then(|state| state.downcast::<tokio::sync::RwLock<T>>().ok())
            .ok_or_else(|| AppError::internal("typed plugin state is unavailable"))
    }

    pub async fn state_read<T: Send + Sync + 'static>(
        &self,
    ) -> Result<tokio::sync::OwnedRwLockReadGuard<T>> {
        Ok(self.state::<T>()?.read_owned().await)
    }

    pub async fn state_write<T: Send + Sync + 'static>(
        &self,
    ) -> Result<tokio::sync::OwnedRwLockWriteGuard<T>> {
        Ok(self.state::<T>()?.write_owned().await)
    }

    pub async fn reply(&self, msg: impl Into<MessageChain>) -> Result<()> {
        let message = msg.into();
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
}

#[doc(hidden)]
pub struct EventHandlerContext {
    pub event: Arc<SystemEvent>,
    pub adapter: Arc<dyn BaseAdapter>,
    pub app_ctx: Arc<Ctx>,
    pub telemetry: Arc<Telemetry>,
    plugin_state: Option<PluginState>,
}

#[doc(hidden)]
impl EventHandlerContext {
    pub(crate) fn __new(
        event: Arc<SystemEvent>,
        adapter: Arc<dyn BaseAdapter>,
        app_ctx: Arc<Ctx>,
        telemetry: Arc<Telemetry>,
        plugin_state: Option<PluginState>,
    ) -> Self {
        Self {
            event,
            adapter,
            app_ctx,
            telemetry,
            plugin_state,
        }
    }

    pub fn state<T: Send + Sync + 'static>(&self) -> Result<Arc<tokio::sync::RwLock<T>>> {
        self.plugin_state
            .clone()
            .and_then(|state| state.downcast::<tokio::sync::RwLock<T>>().ok())
            .ok_or_else(|| AppError::internal("typed plugin state is unavailable"))
    }

    pub async fn state_read<T: Send + Sync + 'static>(
        &self,
    ) -> Result<tokio::sync::OwnedRwLockReadGuard<T>> {
        Ok(self.state::<T>()?.read_owned().await)
    }

    pub async fn state_write<T: Send + Sync + 'static>(
        &self,
    ) -> Result<tokio::sync::OwnedRwLockWriteGuard<T>> {
        Ok(self.state::<T>()?.write_owned().await)
    }
}

#[doc(hidden)]
pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
#[doc(hidden)]
pub type HandlerFunc = Arc<dyn Fn(HandlerContext) -> HandlerFuture + Send + Sync>;

#[doc(hidden)]
pub struct MessageHandler {
    pub id: String,
    pub route: RouteHint,
    pub rule: Option<Rule>,
    pub timeout: Duration,
    pub func: HandlerFunc,
}

#[doc(hidden)]
impl MessageHandler {
    pub fn new(
        id: impl Into<String>,
        route: RouteHint,
        rule: Option<Rule>,
        func: HandlerFunc,
    ) -> Self {
        Self {
            id: id.into(),
            route,
            rule,
            timeout: Duration::from_secs(5),
            func,
        }
    }

    pub fn exact(id: impl Into<String>, patterns: Vec<&str>, func: HandlerFunc) -> Self {
        let route = RouteHint::Exact(patterns.iter().map(|s| s.to_string()).collect());
        Self {
            id: id.into(),
            route,
            rule: Some(is_fullmatch(patterns)),
            timeout: Duration::from_secs(5),
            func,
        }
    }

    pub fn prefix(id: impl Into<String>, prefixes: Vec<&str>, func: HandlerFunc) -> Self {
        let route = RouteHint::Prefix(prefixes.iter().map(|s| s.to_string()).collect());
        Self {
            id: id.into(),
            route,
            rule: Some(is_startswith(prefixes)),
            timeout: Duration::from_secs(5),
            func,
        }
    }

    pub fn keyword(id: impl Into<String>, keywords: Vec<&str>, func: HandlerFunc) -> Self {
        let route = RouteHint::Keyword(keywords.iter().map(|s| s.to_string()).collect());
        Self {
            id: id.into(),
            route,
            rule: Some(is_keywords(keywords)),
            timeout: Duration::from_secs(5),
            func,
        }
    }

    pub fn regex(id: impl Into<String>, pattern: &str, func: HandlerFunc) -> Self {
        Self {
            id: id.into(),
            route: RouteHint::Regex,
            rule: Some(is_regex(pattern)),
            timeout: Duration::from_secs(5),
            func,
        }
    }

    pub fn fallback(id: impl Into<String>, func: HandlerFunc) -> Self {
        Self {
            id: id.into(),
            route: RouteHint::Fallback,
            rule: None,
            timeout: Duration::from_secs(5),
            func,
        }
    }
}

#[doc(hidden)]
pub type EventHandlerFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
#[doc(hidden)]
pub type EventHandlerFunc = Arc<dyn Fn(EventHandlerContext) -> EventHandlerFuture + Send + Sync>;

#[derive(Clone)]
#[doc(hidden)]
pub struct EventHandler {
    pub event_type: String,
    pub id: String,
    pub func: EventHandlerFunc,
    pub rule: Option<Rule>,
}

#[doc(hidden)]
impl EventHandler {
    pub fn new(
        event_type: impl Into<String>,
        id: impl Into<String>,
        func: EventHandlerFunc,
    ) -> Self {
        Self {
            event_type: event_type.into(),
            id: id.into(),
            func,
            rule: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::{Plugin, PluginMetadata};

    struct TestPlugin {
        meta: PluginMetadata,
        handlers: Vec<MessageHandler>,
    }

    impl TestPlugin {
        fn new() -> Self {
            Self {
                meta: PluginMetadata {
                    id: "test".into(),
                    name: "测试插件".into(),
                    ..Default::default()
                },
                handlers: vec![MessageHandler::new(
                    "handler1",
                    RouteHint::Fallback,
                    None,
                    Arc::new(|_: HandlerContext| Box::pin(async { Ok(()) })),
                )],
            }
        }
    }

    impl Plugin for TestPlugin {
        fn metadata(&self) -> &PluginMetadata {
            &self.meta
        }

        fn message_handlers(&self) -> &[MessageHandler] {
            &self.handlers
        }
    }

    #[test]
    fn t2_4_message_handler_construct() {
        let handler = MessageHandler::new(
            "h1",
            RouteHint::Fallback,
            None,
            Arc::new(|_: HandlerContext| Box::pin(async { Ok(()) })),
        );
        assert!(handler.rule.is_none());
    }

    #[test]
    fn t2_20_event_handler_construct() -> anyhow::Result<()> {
        let handler = EventHandler::new(
            "notice",
            "test_event",
            Arc::new(|_| Box::pin(async { Ok(()) })),
        );
        assert_eq!(handler.event_type, "notice");
        assert_eq!(handler.id, "test_event");
        assert!(handler.rule.is_none());
        Ok(())
    }

    #[test]
    fn t2_32_plugin_with_event_handlers() -> anyhow::Result<()> {
        struct EventPlugin {
            meta: PluginMetadata,
        }
        impl Plugin for EventPlugin {
            fn metadata(&self) -> &PluginMetadata {
                &self.meta
            }
            fn event_handlers(&self) -> &[EventHandler] {
                static HANDLERS: std::sync::LazyLock<Vec<EventHandler>> =
                    std::sync::LazyLock::new(|| {
                        vec![EventHandler::new(
                            "notice",
                            "notice_handler",
                            Arc::new(|_| Box::pin(async { Ok(()) })),
                        )]
                    });
                &HANDLERS
            }
        }

        let plugin = EventPlugin {
            meta: PluginMetadata {
                id: "event_test".into(),
                ..Default::default()
            },
        };
        let handlers = plugin.event_handlers();
        assert_eq!(handlers.len(), 1);
        assert_eq!(handlers[0].event_type, "notice");
        Ok(())
    }

    #[test]
    fn t2_37_message_handler_without_rule() -> anyhow::Result<()> {
        let handler = MessageHandler::new(
            "h1",
            RouteHint::Fallback,
            None,
            Arc::new(|_: HandlerContext| Box::pin(async { Ok(()) })),
        );
        assert!(handler.rule.is_none());
        Ok(())
    }

    #[test]
    fn t2_3_plugin_metadata_id_is_stable() {
        let plugin = TestPlugin::new();
        assert_eq!(plugin.metadata().id, "test");
    }
}
