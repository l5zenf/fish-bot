// Make the crate referable by its crate name for proc-macro generated code.
extern crate self as fish_runtime;

pub mod actor;
pub mod bot;
pub mod builder;
pub mod context;
pub mod fish;
pub mod host;
pub mod loader;
pub mod messages;
pub mod plugin;
pub mod prelude;

pub use bot::{Bot, DispatchEvent, DispatchSystemEvent};
pub use actor::PluginActor;
pub use builder::{BuiltPlugin, PluginBuilder};
pub use context::Context;
pub use fish::FishWebSocketAdapter;
pub use host::RuntimeHost;
pub use loader::PluginManager;
pub use messages::{HandleEvent, HandleSystemEvent};
pub use plugin::{
    Capability, EventHandler, EventHandlerContext, EventHandlerFunc, EventHandlerFuture,
    HandlerContext, HandlerFunc, HandlerFuture, MessageHandler, Plugin, PluginManifest,
    PluginMetadata, QueueStrategy, RouteHint, RuntimeConfig, StatefulPlugin, stateful_initial_state,
};
#[doc(hidden)]
pub use plugin::__state_lock_tokio;

pub use fish_core::ctx::Ctx;
pub use fish_core::error::{AppError, Result};
pub use fish_core::event::{MessageEvent, SystemEvent};
pub use fish_core::message::{MessageChain, MessageChainItem, MessageSegment};
pub use fish_core::rule::{MatchList, Rule, is_fullmatch, is_keywords, is_regex, is_startswith};
pub use fish_core::telemetry::Telemetry;
pub use fish_core::{AdapterEventSink, BaseAPI, BaseAdapter};

pub use fish_plugin_macros::{plugin, plugin_handlers};

#[cfg(test)]
mod api_tests {
    use crate as fish_runtime;

    #[test]
    fn runtime_exposes_plugin_author_api() {
        use fish_runtime::prelude::*;
        use fish_runtime::{plugin, plugin_handlers};

        #[plugin(id = "runtime_api", name = "Runtime API")]
        #[derive(Default)]
        struct RuntimeApiPlugin;

        #[plugin_handlers]
        impl RuntimeApiPlugin {
            #[command("/ping")]
            async fn ping(&mut self, _ctx: Context) -> Result<()> {
                Ok(())
            }
        }

        let plugin = RuntimeApiPlugin;
        assert_eq!(plugin.metadata().id, "runtime_api");
        assert_eq!(plugin.message_handlers().len(), 1);
    }
}
