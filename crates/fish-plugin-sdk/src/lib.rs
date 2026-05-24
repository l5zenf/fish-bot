//! fish-plugin-sdk — unified plugin development toolkit.
//!
//! Re-exports the types most plugin authors need, plus builder utilities.
//! Use `fish_plugin_sdk::prelude::*` to bring in the most common items.

// Make the crate referable by its crate name (needed for proc macro generated code
// that references `fish_plugin_sdk::*` when expanded inside this crate's tests).
extern crate self as fish_plugin_sdk;

pub mod builder;
pub mod context;
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

pub use context::Context;

// -- Re-export proc macros --
pub use fish_plugin_macros::{plugin, plugin_handlers};

#[cfg(test)]
mod tests {
    use super::*;

    // ---- New #[plugin] + #[plugin_handlers] tests ----

    #[test]
    fn s6_1_derive_plugin_metadata() {
        #[plugin(id = "test_derive", name = "Test Derive", version = "1.0", description = "a test", author = "me")]
        struct TestPlugin;

        #[plugin_handlers]
        impl TestPlugin {}

        let plugin = TestPlugin;
        assert_eq!(plugin.metadata().id, "test_derive");
        assert_eq!(plugin.metadata().name, "Test Derive");
        assert_eq!(plugin.metadata().version, "1.0");
        assert_eq!(plugin.metadata().description, "a test");
        assert_eq!(plugin.metadata().author, "me");
    }

    #[test]
    fn s6_2_plugin_message_handler() {
        #[plugin(id = "msgh", name = "MsgH")]
        struct MsgPlugin;

        #[plugin_handlers]
        impl MsgPlugin {
            #[command("/ping")]
            async fn ping(&mut self, _ctx: Context) -> Result<()> {
                Ok(())
            }
        }

        let plugin = MsgPlugin;
        assert_eq!(plugin.message_handlers().len(), 1);
        assert_eq!(plugin.message_handlers()[0].id, "ping");
    }

    #[test]
    fn s6_3_plugin_multiple_handlers() {
        #[plugin(id = "multi", name = "Multi")]
        struct MultiPlugin;

        #[plugin_handlers]
        impl MultiPlugin {
            #[command("/a")]
            async fn handler_a(&mut self, _ctx: Context) -> Result<()> { Ok(()) }

            #[command("/b", kind = "prefix")]
            async fn handler_b(&mut self, _ctx: Context) -> Result<()> { Ok(()) }

            #[message(keyword = "alert")]
            async fn handler_c(&mut self, _ctx: Context) -> Result<()> { Ok(()) }
        }

        let plugin = MultiPlugin;
        assert_eq!(plugin.message_handlers().len(), 3);
    }

    #[test]
    fn s6_4_plugin_event_handler() {
        #[plugin(id = "evt", name = "Evt")]
        struct EvtPlugin;

        #[plugin_handlers]
        impl EvtPlugin {
            #[event("order_create")]
            async fn on_order(&mut self, _ctx: Context) -> Result<()> { Ok(()) }
        }

        let plugin = EvtPlugin;
        let handlers = plugin.event_handlers();
        assert_eq!(handlers.len(), 1);
        assert!(handlers.contains_key("order_create"));
    }

    #[test]
    fn s6_5_plugin_regex_and_fallback() {
        #[plugin(id = "rf", name = "RF")]
        struct RFPlugin;

        #[plugin_handlers]
        impl RFPlugin {
            #[command(pattern = r"\d+", kind = "regex")]
            async fn re_h(&mut self, _ctx: Context) -> Result<()> { Ok(()) }

            #[command(fallback)]
            async fn fb_h(&mut self, _ctx: Context) -> Result<()> { Ok(()) }
        }

        let plugin = RFPlugin;
        assert_eq!(plugin.message_handlers().len(), 2);
    }

    #[test]
    fn s6_6_plugin_empty_handlers() {
        #[plugin(id = "empty", name = "Empty")]
        struct EmptyPlugin;

        #[plugin_handlers]
        impl EmptyPlugin {}

        let plugin = EmptyPlugin;
        assert!(plugin.message_handlers().is_empty());
        assert!(plugin.event_handlers().is_empty());
    }

    #[test]
    fn s6_7_plugin_with_registry() {
        #[plugin(id = "reg_derive", name = "RegDerive")]
        struct RegPlugin;

        #[plugin_handlers]
        impl RegPlugin {
            #[command("/ping")]
            async fn ping(&mut self, _ctx: Context) -> Result<()> { Ok(()) }
        }

        register_plugin(RegPlugin);
        let plugins = registered_plugins();
        assert!(plugins.iter().any(|p| p.metadata().id == "reg_derive"));
    }
}
