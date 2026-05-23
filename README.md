# fish-bot

基于 [kameo](https://github.com/tqwewe/kameo) actor 框架的高性能闲鱼机器人。

## 特性

- **actor 隔离** — 每个插件独立 kameo actor，panic 不扩散，慢插件不阻塞快插件
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
use fish_core::event::MessageEvent;
use fish_core::message::MessageSegment;
use fish_core::rule::is_fullmatch;
use fish_core::ctx::Ctx;
use fish_adapter::adapter::BaseAdapter;
use fish_plugin::plugin::{Plugin, PluginMetadata, MessageHandler};
use std::sync::Arc;

pub struct MyPlugin;

impl Plugin for MyPlugin {
    fn metadata(&self) -> PluginMetadata {
        PluginMetadata {
            id: "my_plugin".into(),
            name: "我的插件".into(),
            description: "一个简单的 demo 插件".into(),
            ..Default::default()
        }
    }

    fn message_handlers(&self) -> Vec<MessageHandler> {
        vec![MessageHandler {
            func: Arc::new(|event, _adapter, _ctx| {
                Box::pin(async move {
                    let _ = event.reply(MessageSegment::text("pong")).await;
                })
            }),
            rule: Some(is_fullmatch(["/ping"])),
        }]
    }
}
```

在 `main.rs` 注册：

```rust
fish_plugin::plugin::register_plugin(MyPlugin);
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
