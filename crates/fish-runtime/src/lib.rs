// Make the crate referable by its crate name for proc-macro generated code.
extern crate self as fish_runtime;

pub mod actor;
pub mod bot;
pub mod builder;
pub mod bus;
pub mod context;
pub mod fish;
pub mod handlers;
pub mod host;
pub mod messages;
pub mod plugin;
pub mod prelude;
pub mod runtime;

pub use builder::{ActorMailbox, ActorPlugin, ActorPluginBuilder};
pub use bus::{ActorBus, ActorBusHandle, RuntimeActorBus};
pub use context::{EventContext, MessageContext};
pub use fish::FishWebSocketAdapter;
pub use host::RuntimeHost;
pub use plugin::Plugin;

pub use fish_core::ctx::Ctx;
pub use fish_core::error::{AppError, Result};
pub use fish_core::event::{MessageEvent, SystemEvent};
pub use fish_core::message::{MessageChain, MessageChainItem, MessageSegment};
pub use fish_core::rule::{MatchList, Rule, is_fullmatch, is_keywords, is_regex, is_startswith};
pub use fish_core::telemetry::Telemetry;
pub use fish_core::{AdapterEventSink, BaseAPI, BaseAdapter};

pub use fish_plugin_macros::plugin;

#[cfg(test)]
mod api_tests {
    use crate as fish_runtime;

    #[test]
    fn runtime_exposes_plugin_author_api() {
        use fish_runtime::plugin;
        use fish_runtime::prelude::*;

        struct RuntimeApiPlugin;

        #[plugin]
        impl RuntimeApiPlugin {
            #[message("/ping")]
            async fn ping(&mut self, _ctx: MessageContext) -> Result<()> {
                Ok(())
            }
        }

        let plugin = RuntimeApiPlugin;
        assert_eq!(plugin.metadata().id, "runtime_api_plugin");
        assert_eq!(plugin.metadata().name, "RuntimeApiPlugin");
        assert_eq!(plugin.message_handlers().len(), 1);
    }

    #[test]
    fn runtime_plugin_accepts_rust_init_expression() {
        use fish_runtime::plugin;
        use fish_runtime::prelude::*;

        struct CounterPlugin {
            value: u64,
        }

        #[plugin(Self { value: 7 })]
        impl CounterPlugin {
            #[message("/value")]
            async fn value(&self, _ctx: MessageContext) -> Result<()> {
                Ok(())
            }
        }

        let plugin = CounterPlugin { value: 0 };
        let state = plugin
            .initial_state()
            .expect("stateful plugin should expose initial state");
        let state = state
            .downcast::<tokio::sync::RwLock<CounterPlugin>>()
            .expect("plugin state should downcast to typed lock");
        let state = state
            .try_read()
            .expect("initial state lock should be readable");
        assert_eq!(state.value, 7);
    }
}
