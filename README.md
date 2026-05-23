# fish-bot

基于 [kameo](https://github.com/tqwewe/kameo) actor 框架的高性能闲鱼机器人。

## 特性

- **actor 隔离** — 每个插件独立 kameo actor，panic 不扩散，慢插件不阻塞快插件
- **路由表派发** — Bot 启动时预编译路由表，exact 命令 O(1) 查表直达 handler，不扫全部插件
- **规则引擎** — 组合式规则匹配（前缀/全匹配/关键词/正则 + and/or）
- **依赖注入** — `Ctx` 容器按类型存取，handler 签名统一拿到 `(event, adapter, ctx)`
- **零 unwrap** — `parking_lot` 无锁中毒，`snafu` 结构化错误，无隐式 `From`
- **可扩展适配器** — `BaseAdapter` trait，换平台只替换 adapter 实现

## 快速开始

```bash
mkdir -p data
echo '{"unb":"your_id","_m_h5_tk":"..."}' > data/fish_auth.json
RUST_LOG=info cargo run -p fish-bot
```

首次运行无凭证时自动弹出终端二维码扫码登录。

```bash
# 抑制依赖库 debug 日志
RUST_LOG=info,reqwest=warn,tungstenite=warn cargo run -p fish-bot
```

## 写一个插件

```rust
use std::sync::Arc;
use fish_core::event::MessageEvent;
use fish_core::message::MessageSegment;
use fish_core::rule::is_fullmatch;
use fish_core::ctx::Ctx;
use fish_adapter::adapter::BaseAdapter;
use fish_plugin::plugin::{Plugin, PluginMetadata, MessageHandler, RouteHint};

pub struct MyPlugin {
    metadata: PluginMetadata,
    handlers: Vec<MessageHandler>,
}

impl MyPlugin {
    pub fn new() -> Self {
        Self {
            metadata: PluginMetadata {
                id: "my_plugin".into(),
                name: "我的插件".into(),
                description: "一个简单的 demo 插件".into(),
                ..Default::default()
            },
            handlers: vec![MessageHandler::new(
                "ping",                                    // handler id（日志用）
                RouteHint::Exact(vec!["/ping".into()]),    // 路由提示（Bot 预编译路由表）
                Some(is_fullmatch(["/ping"])),             // 匹配规则
                Arc::new(|event, _adapter, _ctx| {
                    Box::pin(async move {
                        event.reply(MessageSegment::text("pong")).await;
                        Ok(())
                    })
                }),
            )],
        }
    }
}

impl Plugin for MyPlugin {
    fn metadata(&self) -> &PluginMetadata {
        &self.metadata
    }

    fn message_handlers(&self) -> &[MessageHandler] {
        &self.handlers
    }
}
```

在 `main.rs` 注册：

```rust
fish_plugin::plugin::register_plugin(MyPlugin::new());
```

## 路由提示

每个 `MessageHandler` 需要指定 `RouteHint`，告诉 Bot 如何索引这个 handler。Bot 启动时预编译路由表，消息到达时 O(1) 查表直达 handler，不再遍历所有插件。

```rust
pub enum RouteHint {
    Exact(Vec<String>),   // 精确匹配，Bot 用 HashMap 索引
    Prefix(Vec<String>),  // 前缀匹配，Bot 遍历前缀列表
    Keyword(Vec<String>), // 关键词匹配，Bot 遍历关键词列表
    Regex,                // 正则匹配，Bot 无法预过滤，由 PluginActor 检查规则
    Fallback,             // 无条件派发，Bot 总是转发给 PluginActor，规则交 PluginActor 检查
}
```

`RouteHint` 应与 `Rule` 一致。例如 `/ping` 用 `is_fullmatch` → `RouteHint::Exact`，Bot 验证后跳过 PluginActor 的重复规则检查。

```rust
// RouteHint 与 Rule 对应关系：
is_fullmatch("/ping")      → RouteHint::Exact(["ping"])
is_startswith("/admin")    → RouteHint::Prefix(["/admin"])
is_keywords("delete")      → RouteHint::Keyword(["delete"])
is_regex("...")            → RouteHint::Regex
                        → RouteHint::Fallback  // 无规则或复杂组合
```

## 规则组合

```rust
is_startswith("/admin").and(&is_keywords("delete"))
is_fullmatch(["/help", "/h", "帮助"])
is_regex(r"^\d{11}$").or(&is_fullmatch(["/phone"]))
```

## 依赖注入

```rust
// main.rs 启动时注入
let ctx = Arc::new(Ctx::new());
ctx.insert(my_db_pool);

// handler 中按类型取出
let pool = ctx.get::<PgPool>();
```

## License

MIT OR Apache-2.0
