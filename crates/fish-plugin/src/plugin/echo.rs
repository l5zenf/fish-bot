use std::sync::Arc;

use fish_adapter::adapter::BaseAdapter;
use fish_core::ctx::Ctx;
use fish_core::event::MessageEvent;
use fish_core::message::MessageSegment;
use crate::plugin::{MessageHandler, Plugin, PluginMetadata};
use fish_core::rule::is_fullmatch;

/// Echo plugin matching Python builtin_plugins/echo.py.
pub struct EchoPlugin;

impl EchoPlugin {
    pub fn new() -> Self {
        Self
    }
}

impl Plugin for EchoPlugin {
    fn metadata(&self) -> PluginMetadata {
        PluginMetadata {
            id: "echo".into(),
            name: "回声插件".into(),
            description: "一个简单的回声插件，用于演示自动回复功能".into(),
            version: "1.0.0".into(),
            author: "Kaguya233qwq".into(),
        }
    }

    fn message_handlers(&self) -> Vec<MessageHandler> {
        vec![MessageHandler {
            func: Arc::new(
                |event: MessageEvent, _adapter: Arc<dyn BaseAdapter>, _ctx: Arc<Ctx>| {
                    Box::pin(async move {
                        let content = event.plain_text().trim().to_string();
                        let reply_msg = format!("Echo: {}", content);
                        let _ = event.reply(MessageSegment::text(reply_msg)).await;
                    })
                },
            ),
            rule: Some(is_fullmatch(["/echo"])),
        }]
    }
}

impl Default for EchoPlugin {
    fn default() -> Self {
        Self::new()
    }
}
