# fish-bot

基于 [kameo](https://github.com/tqwewe/kameo) actor 框架的高性能闲鱼机器人。

## 特性

- **actor 隔离** — 每个插件独立 kameo actor，panic 不扩散，慢插件不阻塞快插件
- **预编译路由表** — Bot 启动时按 `RouteHint` 编译路由表，精确匹配 `O(1)`，无需遍历所有插件
- **规则引擎** — 组合式规则：前缀 / 全匹配 / 关键词 / 正则，支持 `and` / `or` 复合
- **单结构体 HandlerContext** — handler 统一接收 `HandlerContext { event, adapter, app_ctx, telemetry }`，不再分散三个参数
- **可观测指标** — 18 项原子计数器（路由命中 / 派发 / 回复失败 / handler 耗时等），60s 自动输出
- **队列策略** — 每个 PluginActor 独立 `QueueStrategy`，支持 `DropNewest`（默认）与 `DropOldest`
- **零 unwrap** — `parking_lot` 无锁中毒，`snafu` 结构化错误，无隐式 `From`
- **MTOP 协议** — 完整实现闲鱼 MTOP 签名、WebSocket 注册 / 心跳、同步、收发消息
- **终端二维码登录** — 无凭证时自动弹出二维码，扫码即登录

## 快速开始

```bash
RUST_LOG=info cargo run -p fish-bot
```

首次运行无凭证时自动弹出终端二维码扫码登录。

```bash
# 抑制依赖库 debug 日志
RUST_LOG=info,reqwest=warn,tungstenite=warn cargo run -p fish-bot
```

## 架构

```
adapter (平台) → Bot (路由 & 派发) → PluginActor (handler 执行)
    │                    │                       ├── QueueStrategy
    │                    │                       └── Telemetry
    ├── FishAPI          ├── exact_routes (HashMap)
    ├── FishConnection   ├── prefix_routes
    └── AuthManager      ├── keyword_routes
                         └── fallback_routes
```

- **Adapter** — 与闲鱼 WebSocket 交互：登录、连接、心跳、编解码
- **Bot** — 消息入口，按预编译路由表 `O(1)` ~ `O(n)` 派发到 PluginActor
- **PluginActor** — 每个插件独立 actor，执行 handler，超时 / panic 不波及其它插件

## 写一个插件

```rust
use std::sync::Arc;
use fish_plugin::plugin::{Plugin, PluginMetadata, MessageHandler, RouteHint, HandlerContext};
use fish_core::message::MessageSegment;

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
            handlers: vec![
                // RouteHint::Exact 会被 Bot 编入 HashMap，O(1) 路由
                MessageHandler::exact(
                    "ping",
                    vec!["/ping"],
                    Arc::new(|cx: HandlerContext| {
                        Box::pin(async move {
                            cx.event.reply(MessageSegment::text("pong")).await;
                            Ok(())
                        })
                    }),
                ),
                // RouteHint::Prefix 走前缀匹配
                MessageHandler::prefix(
                    "admin",
                    vec!["/admin"],
                    Arc::new(|cx: HandlerContext| {
                        Box::pin(async move {
                            cx.event.reply(MessageSegment::text("admin cmd")).await;
                            Ok(())
                        })
                    }),
                ),
            ],
        }
    }
}

impl Plugin for MyPlugin {
    fn metadata(&self) -> &PluginMetadata { &self.metadata }
    fn message_handlers(&self) -> &[MessageHandler] { &self.handlers }
}
```

注册：

```rust
fish_plugin::plugin::register_plugin(MyPlugin::new());
```

### RouteHint 路线提示

`RouteHint` 告诉 Bot 如何索引 handler，不参与实际匹配（那是 `Rule` 的事）。但两者应一致：

| RouteHint | 对应 Rule | 路由成本 |
|---|---|---|
| `Exact(["msg"])` | `is_fullmatch(["msg"])` | O(1) HashMap |
| `Prefix(["/admin"])` | `is_startswith("/admin")` | O(n) 遍历 |
| `Keyword(["delete"])` | `is_keywords(["delete"])` | O(n) 遍历 |
| `Regex` | `is_regex(r"...")` | 无条件派发，PluginActor 自行检查 |
| `Fallback` | 无规则或复杂组合 | 无条件派发，PluginActor 自行检查 |

```rust
// 各种构造方式
MessageHandler::exact("id", vec!["/ping"], handler)
MessageHandler::prefix("id", vec!["/admin"], handler)
MessageHandler::keyword("id", vec!["delete"], handler)
MessageHandler::regex("id", r"^\d{11}$", handler)
MessageHandler::fallback("id", handler)           // 无前置规则
MessageHandler::new("id", RouteHint::Fallback, Some(rule), handler)  // 自定义规则
```

## HandlerContext

Handler 统一接收 `HandlerContext`，包含四个字段：

```rust
pub struct HandlerContext {
    pub event: MessageEvent,         // 消息事件（reply / plain_text）
    pub adapter: Arc<dyn BaseAdapter>, // 发送消息的 adapter
    pub app_ctx: Arc<Ctx>,           // 应用级依赖注入容器
    pub telemetry: Arc<Telemetry>,   // 可观测指标
}
```

## 队列策略

每个 `PluginActor` 支持通过 `QueueStrategy` 控制并发排队行为：

```rust
QueueStrategy::DropNewest  // (默认)队列满时丢弃新事件
QueueStrategy::DropOldest  // 队列满时丢弃最旧事件，为新事件腾位
```

设置：

```rust
PluginActor::with_config(plugin, QueueStrategy::DropOldest)
```

## 可观测指标

60 秒自动输出一次摘要。指标分为三层：

- **路由层**: `messages_received`, `exact_route_hits`, `unmatched_messages`, `handler_dispatches` ...
- **Handler 层**: `handler_started`, `handler_succeeded`, `handler_failed`, `handler_timed_out`
- **队列层**: `drop_newest_drops`, `drop_oldest_enqueues`, `queued_handler_succeeded` ...

Handler 内可通过 `cx.telemetry` 访问计数器。

## 依赖注入

```rust
// main.rs 启动时注入
let ctx = Arc::new(Ctx::new());
ctx.insert(my_db_pool);

// handler 中按类型取出
let pool = cx.app_ctx.get::<PgPool>();
```

## 规则组合

```rust
use fish_core::rule::*;

is_startswith("/admin").and(&is_keywords("delete"))
is_fullmatch(["/help", "/h", "帮助"])
is_regex(r"^\d{11}$").or(&is_fullmatch(["/phone"]))
```

## License

MIT OR Apache-2.0
