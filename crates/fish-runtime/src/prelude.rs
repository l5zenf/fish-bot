//! Prelude — glob import the most commonly used types.
//!
//! ```rust
//! use fish_runtime::prelude::*;
//! ```

pub use crate::{
    ActorBusHandle, ActorPluginBuilder, Ctx, EventContext, MessageChain, MessageChainItem,
    MessageContext, MessageSegment, Plugin, QueueStrategy, Result, RuntimeHost,
};
