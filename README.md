# fish-bot

基于 [kameo](https://github.com/tqwewe/kameo) actor 框架的高性能闲鱼机器人。

## 架构

```
workspace/
  fish-core       — 消息模型、事件、规则引擎、依赖容器、错误类型
  fish-adapter    — 通信适配器 trait + 闲鱼 WebSocket 实现
  fish-plugin     — 插件 trait + kameo PluginActor + 内置 echo 插件
  fish-bot        — BotActor (kameo actor) + main 入口
```

### 数据流

```
闲鱼 WebSocket ──► FishWebSocketAdapter ──► BotActor (fan-out)
                                                   │
                                     tell ┌────────┼────────┐
                                          ▼        ▼        ▼
                                   PluginActor  PluginActor  PluginActor
                                   (规则匹配)    (规则匹配)    (规则匹配)
```

## 快速开始

```bash
# 准备闲鱼登录凭证
mkdir -p data
echo '{"unb":"your_user_id","_m_h5_tk":"..."}' > data/fish_auth.json

# 启动
RUST_LOG=info cargo run -p fish-bot
```

首次运行无凭证时会自动进入二维码扫码登录流程。

## 控制日志

```bash
# 只看 info 及以上
RUST_LOG=info cargo run -p fish-bot

# 抑制依赖库噪音
RUST_LOG=info,reqwest=warn,tungstenite=warn,tokio_tungstenite=warn cargo run -p fish-bot
```

## 插件开发

实现 `Plugin` trait，注册即可：

```rust
use fish_core::event::MessageEvent;
use fish_core::message::MessageSegment;
use fish_core::rule::is_fullmatch;
use fish_adapter::adapter::BaseAdapter;
use fish_core::ctx::Ctx;
use fish_plugin::plugin::{Plugin, PluginMetadata, MessageHandler};
use std::sync::Arc;

pub struct MyPlugin;

impl Plugin for MyPlugin {
    fn metadata(&self) -> PluginMetadata {
        PluginMetadata {
            id: "my_plugin".into(),
            name: "我的插件".into(),
            description: "...".into(),
            version: "1.0.0".into(),
            author: "...".into(),
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

然后在 `main.rs` 中注册：

```rust
use fish_plugin::plugin::register_plugin;
register_plugin(MyPlugin::new());
```

### 依赖注入

通过 `Ctx` 共享依赖（DB 连接池、配置等）：

```rust
// main.rs
let ctx = Arc::new(Ctx::new());
ctx.insert(my_db_pool);
ctx.insert(my_config);

// handler 中取出
|event, adapter, ctx| {
    let pool = ctx.get::<PgPool>().unwrap();
    // ...
}
```

### 规则匹配

```rust
use fish_core::rule::{is_startswith, is_fullmatch, is_keywords, is_regex};

// 单匹配
is_fullmatch("/help")
// 多匹配
is_fullmatch(["/help", "/h", "帮助"])
// 组合
is_startswith("/admin").and(&is_keywords("delete"))
```

## 项目结构

```
crates/
  fish-core/src/
    message.rs       — MessageSegment (Text/Image/Audio/CustomNode) + MessageChain
    event.rs         — MessageEvent (cid, sender, messages, reply())
    rule.rs          — Rule (and/or 组合, is_startswith/is_fullmatch/is_regex)
    ctx.rs           — Arc<dyn Any> 依赖注入容器
    error.rs         — AppError (snafu 错误类型)
  fish-adapter/src/
    adapter.rs       — BaseAdapter trait + BaseAPI
    fish.rs          — FishWebSocketAdapter (WS 连接、ACK、心跳、解密)
    fish/api.rs      — FishAPI (MTOP 协议封装)
    fish/auth.rs     — AuthManager (Cookie 持久化)
    fish/sign.rs     — 签名/解密/MID/UUID 生成
    fish/protocol.rs — 闲鱼消息编解码
  fish-plugin/src/
    plugin.rs        — Plugin trait + MessageHandler + 全局注册表
    plugin/actor.rs  — PluginActor (kameo actor, 规则匹配 + handler 执行)
    plugin/echo.rs   — EchoPlugin (示例插件)
    loader.rs        — PluginManager
  fish-bot/src/
    main.rs          — 启动入口
    bot.rs           — BotActor (fan-out 到所有 PluginActor)
    bootstrap.rs     — tracing 初始化 (RUST_LOG)
```

## 设计原则

- **零 unwrap**: 全部用 `parking_lot`（无锁中毒）、`HeaderValue::from_static`、`unwrap_or_default`
- **低耦合**: 核心 trait 稳定，插件只依赖 `Plugin` trait + `Ctx`
- **snafu 错误处理**: 结构化错误上下文，无 `thiserror` 的 `#[from]` 隐式耦合
- **actor 隔离**: 每个 plugin 独立 kameo actor，panic 不扩散
