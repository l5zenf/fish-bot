//! fish-plugin-sdk — unified plugin development toolkit.
//!
//! Re-exports the types most plugin authors need, plus builder utilities.
//! Use `fish_plugin_sdk::prelude::*` to bring in the most common items.

pub mod builder;
pub mod prelude;

// -- Re-exports for convenience --

pub use fish_core::ctx::Ctx;
pub use fish_core::error::{AppError, Result};
pub use fish_core::event::{MessageEvent, SystemEvent};
pub use fish_core::message::{MessageChain, MessageChainItem, MessageSegment};
pub use fish_core::rule::{
    is_fullmatch, is_keywords, is_regex, is_startswith, MatchList, Rule,
};
pub use fish_core::telemetry::Telemetry;

pub use fish_adapter::adapter::BaseAdapter;

pub use fish_plugin::plugin::{
    Capability, EventHandler, EventHandlerFunc, EventHandlerFuture, HandlerContext, HandlerFunc,
    HandlerFuture, MessageHandler, Plugin, PluginManifest, PluginMetadata, QueueStrategy,
    RouteHint, RuntimeConfig, StatefulPlugin, stateful_initial_state,
    register_plugin, registered_plugins,
};

pub use fish_runtime::{HandleEvent, HandleSystemEvent, PluginActor};

pub use builder::{BuiltPlugin, PluginBuilder};

pub use fish_plugin_macros::Plugin;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn s6_1_derive_plugin_metadata() {
        #[derive(Plugin)]
        #[plugin(id = "test_derive", name = "Test Derive", version = "1.0", description = "a test", author = "me")]
        struct TestPlugin;

        let plugin = TestPlugin;
        assert_eq!(plugin.metadata().id, "test_derive");
        assert_eq!(plugin.metadata().name, "Test Derive");
        assert_eq!(plugin.metadata().version, "1.0");
        assert_eq!(plugin.metadata().description, "a test");
        assert_eq!(plugin.metadata().author, "me");
    }

    #[test]
    fn s6_2_derive_plugin_message_handler() {
        #[derive(Plugin)]
        #[plugin(id = "msgh", name = "MsgH")]
        #[command_handler(id = "ping", pattern = "/ping", func = ping_h)]
        struct MsgPlugin;

        async fn ping_h(_cx: HandlerContext) -> Result<()> { Ok(()) }

        let plugin = MsgPlugin;
        assert_eq!(plugin.message_handlers().len(), 1);
        assert_eq!(plugin.message_handlers()[0].id, "ping");
    }

    #[test]
    fn s6_3_derive_plugin_multiple_handlers() {
        #[derive(Plugin)]
        #[plugin(id = "multi", name = "Multi")]
        #[command_handler(id = "h1", pattern = "/a", func = handler_a)]
        #[command_handler(id = "h2", pattern = "/b", kind = "prefix", func = handler_b)]
        #[command_handler(id = "h3", pattern = "alert", kind = "keyword", func = handler_c)]
        struct MultiPlugin;

        async fn handler_a(_cx: HandlerContext) -> Result<()> { Ok(()) }
        async fn handler_b(_cx: HandlerContext) -> Result<()> { Ok(()) }
        async fn handler_c(_cx: HandlerContext) -> Result<()> { Ok(()) }

        let plugin = MultiPlugin;
        assert_eq!(plugin.message_handlers().len(), 3);
    }

    #[test]
    fn s6_4_derive_plugin_event_handler() {
        #[derive(Plugin)]
        #[plugin(id = "evt", name = "Evt")]
        #[event_handler(id = "e1", event_type = "order_create", func = on_order)]
        struct EvtPlugin;

        async fn on_order(_event: Arc<SystemEvent>, _adapter: Arc<dyn BaseAdapter>, _ctx: Arc<Ctx>) -> Result<()> { Ok(()) }

        let plugin = EvtPlugin;
        let handlers = plugin.event_handlers();
        assert_eq!(handlers.len(), 1);
        assert!(handlers.contains_key("order_create"));
    }

    #[test]
    fn s6_5_derive_plugin_regex_and_fallback() {
        #[derive(Plugin)]
        #[plugin(id = "rf", name = "RF")]
        #[command_handler(id = "re", pattern = r"\d+", kind = "regex", func = re_h)]
        #[command_handler(id = "fb", pattern = "/fb", kind = "fallback", func = fb_h)]
        struct RFPlugin;

        async fn re_h(_cx: HandlerContext) -> Result<()> { Ok(()) }
        async fn fb_h(_cx: HandlerContext) -> Result<()> { Ok(()) }

        let plugin = RFPlugin;
        assert_eq!(plugin.message_handlers().len(), 2);
    }

    #[test]
    fn s6_6_derive_plugin_empty_handlers() {
        #[derive(Plugin)]
        #[plugin(id = "empty", name = "Empty")]
        struct EmptyPlugin;

        let plugin = EmptyPlugin;
        assert!(plugin.message_handlers().is_empty());
        assert!(plugin.event_handlers().is_empty());
    }

    #[test]
    fn s6_7_derive_plugin_with_registry() {
        #[derive(Plugin)]
        #[plugin(id = "reg_derive", name = "RegDerive")]
        #[command_handler(id = "ping", pattern = "/ping", func = ping_d)]
        struct RegPlugin;

        async fn ping_d(_cx: HandlerContext) -> Result<()> { Ok(()) }

        register_plugin(RegPlugin);
        let plugins = registered_plugins();
        assert!(plugins.iter().any(|p| p.metadata().id == "reg_derive"));
    }
}
