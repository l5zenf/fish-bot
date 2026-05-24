# fish-bot

基于 [kameo](https://github.com/tqwewe/kameo) actor 框架的高性能闲鱼机器人。

## 特性

- **actor 隔离** — 每个插件独立 kameo actor，panic 不扩散，慢插件不阻塞快插件
- **预编译路由表** — Bot 启动时按 `RouteHint` 编译路由表，精确匹配 `O(1)`，无需遍历所有插件
- **规则引擎** — 组合式规则：前缀 / 全匹配 / 关键词 / 正则，支持 `and` / `or` 复合
- **StatefulPlugin** — 插件可在 handler 间保持可变状态，通过 `HandlerContext::state()` 访问
- **PluginBuilder / #[derive(Plugin)]** — 两种方式快速构建插件，无需手写 trait impl
- **可观测指标** — 18 项原子计数器（路由命中 / 派发 / 回复失败 / handler 耗时等），60s 自动输出
- **队列策略** — 每个 PluginActor 独立 `QueueStrategy`，支持 `DropNewest`（默认）与 `DropOldest`
- **零 unwrap** — `parking_lot` 无锁中毒，`snafu` 结构化错误，无隐式 `From`
- **MTOP 协议** — 完整实现闲鱼 MTOP 签名、WebSocket 注册 / 心跳、同步、收发消息
- **终端二维码登录** — 无凭证时自动弹出二维码，扫码即登录
- **SystemEvent 事件处理** — 非聊天消息（下单、拍下、系统通知）自动分类，按事件类型派发
- **MessagePack 协议** — 支持 base64 → msgpack → JSON 解密路径，对齐闲鱼协议
- **协议消息过滤** — 自动过滤输入状态（typing）和系统推送（needPush=false），仅处理有效业务消息
- **红提醒分类** — 将闲鱼 `redReminder` 推送自动映射为 `order_create` / `order_closed` / `item_purchased` 等业务事件

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
                         ├── fallback_routes
                         └── event_routes (HashMap) ─── SystemEvent → PluginActor (EventHandler)
```

Crate 依赖关系：

```
fish-bot → fish-runtime → fish-plugin-sdk → fish-plugin → fish-adapter → fish-core
fish-bot → fish-adapter → fish-core
fish-plugin-macros (proc-macro, 独立 crate)
```

- **Adapter** — 与闲鱼 WebSocket 交互：登录、连接、心跳、编解码；聊天消息走 `callback`，非聊天事件走 `event_callback`
- **Bot** — 消息入口，按预编译路由表 `O(1)` ~ `O(n)` 派发到 PluginActor；`event_routes` 按事件类型路由业务事件
- **PluginActor** — 每个插件独立 actor，执行 handler，超时 / panic 不波及其它插件
- **fish-plugin-sdk** — 插件开发一站式入口，统一 re-export 所有常用类型
- **fish-plugin-macros** — 提供 `#[derive(Plugin)]` proc macro，零样板代码实现 Plugin trait

## 写一个插件

fish-bot 提供两种方式编写插件，任选其一。

### 方式一：#[derive(Plugin)]（推荐）

使用 proc macro 自动生成 `Plugin` trait 实现：

```rust
use fish_plugin_sdk::prelude::*;

#[derive(Plugin)]
#[plugin(id = "my_plugin", name = "我的插件", description = "a demo plugin")]
#[command_handler(id = "ping", pattern = "/ping", func = ping_handler)]
#[command_handler(id = "admin", pattern = "/admin", kind = "prefix", func = admin_handler)]
#[event_handler(id = "notify", event_type = "order_create", func = on_order)]
struct MyPlugin;

async fn ping_handler(cx: HandlerContext) -> Result<()> {
    cx.event.reply(MessageSegment::text("pong")).await;
    Ok(())
}

async fn admin_handler(cx: HandlerContext) -> Result<()> {
    cx.event.reply(MessageSegment::text("admin cmd")).await;
    Ok(())
}

async fn on_order(event: Arc<SystemEvent>, adapter: Arc<dyn BaseAdapter>, ctx: Arc<Ctx>) -> Result<()> {
    tracing::info!("order event: {:?}", event.payload);
    let _ = adapter.send("buyer", &MessageChain::from("感谢下单！"), None).await;
    Ok(())
}
```

注册：

```rust
register_plugin(MyPlugin);
```

`#[command_handler]` 支持 `kind` 字段，可选：`exact`（默认）、`prefix`、`keyword`、`regex`、`fallback`。

### 方式二：PluginBuilder

链式 API 构建，适合动态注册或简单的插件：

```rust
use fish_plugin_sdk::prelude::*;
use std::sync::Arc;

PluginBuilder::new("my_plugin", "我的插件")
    .description("a demo plugin")
    .command("ping", "/ping", Arc::new(|cx: HandlerContext| {
        Box::pin(async move {
            cx.event.reply(MessageSegment::text("pong")).await;
            Ok(())
        })
    }))
    .prefix("admin", "/admin", Arc::new(|cx: HandlerContext| {
        Box::pin(async move {
            cx.event.reply(MessageSegment::text("admin cmd")).await;
            Ok(())
        })
    }))
    .on_event("order_create", "notify", Arc::new(|event, adapter, _ctx| {
        Box::pin(async move {
            tracing::info!("order event: {:?}", event.payload);
            let _ = adapter.send("buyer", &MessageChain::from("感谢下单！"), None).await;
            Ok(())
        })
    }))
    .build()
    .register();
```

## 有状态插件

插件可在 handler 间保持可变状态。实现 `StatefulPlugin` trait，通过 `HandlerContext::state()` 访问：

```rust
use fish_plugin_sdk::prelude::*;

#[derive(Plugin)]
#[plugin(id = "counter", name = "Counter")]
#[command_handler(id = "incr", pattern = "/incr", func = incr_handler)]
struct CounterPlugin;

impl StatefulPlugin for CounterPlugin {
    type State = usize;

    fn create_initial_state(&self) -> Self::State {
        0
    }
}

// 也可以使用 #[plugin_state] 属性自动生成（TODO）

async fn incr_handler(cx: HandlerContext) -> Result<()> {
    if let Some(state) = cx.state::<usize>() {
        let mut val = state.write();
        *val += 1;
        cx.event.reply(MessageSegment::text(format!("Count: {}", *val))).await;
    }
    Ok(())
}
```

或使用 `PluginBuilder` 的 `.state()` 方法：

```rust
PluginBuilder::new("counter", "Counter")
    .state(0usize)
    .command("incr", "/incr", Arc::new(|cx: HandlerContext| {
        Box::pin(async move {
            if let Some(state) = cx.state::<usize>() {
                let mut val = state.write();
                *val += 1;
            }
            Ok(())
        })
    }))
    .build()
    .register();
```

## Manifest / Capability / RuntimeConfig

插件可声明能力（Capability）、运行时配置和资源清单：

```rust
PluginBuilder::new("my_plugin", "我的插件")
    .capability(Capability::Network)
    .capability(Capability::SendMessage)
    .concurrency(16)
    .timeout(Duration::from_secs(10))
    .queue_strategy(QueueStrategy::DropOldest(100))
    .build();
```

| Capability | 说明 |
|---|---|
| `Network` | 可发起 HTTP 请求 |
| `FileSystem` | 可读本地文件 |
| `FileSystemWrite` | 可写本地文件 |
| `SendMessage` | 可通过 adapter 发消息 |
| `ReadAppContext` | 可访问应用级 Ctx |

## RouteHint 路线提示

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

Handler 统一接收 `HandlerContext`，包含：

```rust
pub struct HandlerContext {
    pub event: MessageEvent,                          // 消息事件（reply / plain_text）
    pub adapter: Arc<dyn BaseAdapter>,                // 发送消息的 adapter
    pub app_ctx: Arc<Ctx>,                            // 应用级依赖注入容器
    pub telemetry: Arc<Telemetry>,                    // 可观测指标
    pub plugin_state: Option<Arc<dyn Any + Send + Sync>>, // 插件状态（有状态插件）
}
```

访问状态：

```rust
fn state<T: Any + Send + Sync>(&self) -> Option<&RwLock<T>>
```

## SystemEvent 事件处理

除了响应聊天消息，插件还可以监听业务事件（下单、拍下、系统通知等）。这些事件来自 WebSocket 的非聊天推送，由 Adapter 的 `classify_event_type()` 自动分类后派发。

### EventHandler

```rust
pub type EventHandlerFunc = Arc<
    dyn Fn(Arc<SystemEvent>, Arc<dyn BaseAdapter>, Arc<Ctx>) -> EventHandlerFuture + Send + Sync,
>;

pub struct EventHandler {
    pub id: String,
    pub func: EventHandlerFunc,
    pub rule: Option<Rule>,
}
```

### SystemEvent 结构

```rust
pub struct SystemEvent {
    pub event_type: String,         // 事件类型（由 classify_event_type 提取）
    pub payload: Arc<serde_json::Value>,  // 原始业务数据
}
```

### redReminder 事件映射

闲鱼的业务推送通过 `redReminder` 通道下发。`classify_event_type()` 自动映射：

| redReminder 类型 | 映射事件 | 说明 |
|---|---|---|
| `1` | `order_create` | 买家下单 |
| `2` | `order_closed` | 交易关闭 |
| `3` | `item_purchased` | 商品售出 |

## 协议层

### 消息解密流程

```
base64 解码 → msgpack 解包 → JSON 解析（含嵌套解密）
```

1. **base64 解码** — 先尝试 base64 解码原始消息
2. **msgpack 解包** — 解码后尝试用 `rmp-serde` 反序列化为 JSON Value
3. **JSON 解析** — msgpack 解析失败时退化为直接 JSON 解析
4. **嵌套解密** — 对特定字段递归执行 base64 → msgpack → JSON

### 消息过滤

```rust
// 1. 过滤输入状态（typing indicator）
if is_typing_status(&message) { return; }

// 2. 过滤系统推送（needPush == "false"）
if is_system_message(&message) { return; }

// 3. 分类处理
//    - 有会话信息 → MessageEvent → callback
//    - 无会话信息 → classify_event_type → SystemEvent → event_callback
```

## 队列策略

每个 `PluginActor` 支持通过 `QueueStrategy` 控制并发排队行为：

```rust
QueueStrategy::DropNewest  // (默认)队列满时丢弃新事件
QueueStrategy::DropOldest(usize)  // 队列满时丢弃最旧事件，为新事件腾位
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
