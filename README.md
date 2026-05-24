# fish-bot

基于 [kameo](https://github.com/tqwewe/kameo) actor 框架的高性能闲鱼机器人。

## 特性

- **actor 隔离** — 每个插件独立 kameo actor，panic 不扩散，慢插件不阻塞快插件
- **自持有状态** — 插件状态就是 struct 字段，handler 方法通过 `&self` / `&mut self` 访问，零样板
- **预编译路由表** — Bot 启动时按 `RouteHint` 编译路由表，精确匹配 `O(1)`，无需遍历所有插件
- **规则引擎** — 组合式规则：前缀 / 全匹配 / 关键词 / 正则，支持 `and` / `or` 复合
- **PluginBuilder / proc macro** — 两种方式构建插件，无需手写 trait impl
- **可观测指标** — 18 项原子计数器（路由命中 / 派发 / 回复失败 / handler 耗时等），60s 自动输出
- **队列策略** — 每个 PluginActor 独立 `QueueStrategy`，支持 `DropNewest`（默认）与 `DropOldest`
- **零 unwrap** — `parking_lot` 无锁中毒，结构化错误，无隐式 `From`
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
- **PluginActor** — 每个插件独立 actor，执行 handler；根据 handler 签名自动选择 read / write 锁，状态安全无手动管理
- **fish-plugin-sdk** — 插件开发一站式入口，统一 re-export 所有常用类型
- **fish-plugin-macros** — 提供 `#[plugin]` + `#[plugin_handlers]` proc macro，零样板代码实现 Plugin trait

## 写一个插件

使用两个 proc macro 即可完成定义：

```rust
use fish_plugin_sdk::prelude::*;

#[plugin(id = "seller_assistant", name = "卖家助手")]
struct SellerAssistant;

#[plugin_handlers]
impl SellerAssistant {
    #[command("/ping")]
    async fn ping(&self, ctx: Context) -> Result<()> {
        ctx.reply("pong").await
    }
}

register_plugin(SellerAssistant);
```

### 三种 handler 签名

handler 方法的 receiver 决定执行行为：

| 签名 | 并发 | 语义 |
|---|---|---|
| `async fn ping(ctx)` | 完全并发 | 无状态，不访问 struct 字段 |
| `async fn ping(&self, ctx)` | 并发读 | 只读状态，tokio RwLock read 锁 |
| `async fn ping(&mut self, ctx)` | 串行 | 可变状态，tokio RwLock write 锁 |

三种签名可以在同一个 `#[plugin_handlers]` impl 块中混用，actor 系统自动根据签名选择对应的锁操作。

### 有状态插件

状态就是 struct 的普通字段：

```rust
#[plugin(id = "counter", name = "计数器")]
#[derive(Default)]
struct Counter {
    count: u64,
}

#[plugin_handlers]
impl Counter {
    #[command("/incr")]
    async fn incr(&mut self, ctx: Context) -> Result<()> {
        self.count += 1;
        ctx.reply(format!("Count: {}", self.count)).await
    }

    #[command("/count")]
    async fn read(&self, ctx: Context) -> Result<()> {
        ctx.reply(format!("Current: {}", self.count)).await
    }
}
```

`&mut self` 的 `incr` 自动串行化，同一时刻只有一个 handler 写入。`&self` 的 `read` 可以并发执行。

### 命令属性

`#[command(...)]` 支持多种匹配模式：

```rust
#[command("/ping")]              // 精确匹配（默认）
#[command("/admin", kind = "prefix")]  // 前缀匹配
#[command(pattern = r"\d+", kind = "regex")]  // 正则匹配
#[command(fallback)]             // 兜底（无其他 handler 匹配时执行）
```

### 关键词消息

```rust
#[message(keyword = "最低多少钱")]
async fn bargain(&mut self, ctx: Context) -> Result<()> {
    ctx.reply("已经最低了").await
}
```

### 事件处理

```rust
#[event("order_create")]
async fn on_order(&mut self, ctx: Context) -> Result<()> {
    tracing::info!("new order: {:?}", ctx.event());
    Ok(())
}
```

## Context

handler 统一接收 `Context`，提供便捷方法：

```rust
ctx.reply("hello").await?;          // 回复消息
ctx.sender_id();                     // 发送者 ID
ctx.cid();                           // 会话 ID
ctx.text();                          // 消息纯文本
ctx.adapter();                       // 发送消息的 adapter
ctx.app_ctx();                       // 应用级依赖注入容器
ctx.event();                         // 原始 MessageEvent
ctx.telemetry();                     // 可观测指标
```

## PluginBuilder（免 macro 方式）

适合动态注册或简单插件，效果完全等价于 proc macro：

```rust
use fish_plugin_sdk::prelude::*;
use std::sync::Arc;

PluginBuilder::new("echo", "Echo")
    .command("echo", "/echo", Arc::new(|ctx: fish_plugin_sdk::HandlerContext| {
        Box::pin(async move {
            let ctx = fish_plugin_sdk::Context::new(ctx);
            ctx.reply(ctx.text()).await
        })
    }))
    .build()
    .register();
```

### 有状态 + Builder

```rust
PluginBuilder::new("counter", "Counter")
    .state(0u64)
    .command("incr", "/incr", Arc::new(|ctx: HandlerContext| {
        Box::pin(async move {
            let plugin_state = ctx.plugin_state.clone().expect("stateful");
            let lock = plugin_state.downcast::<tokio::sync::RwLock<u64>>().expect("type");
            let mut val = lock.write().await;
            *val += 1;
            ctx.event.reply(MessageSegment::text(format!("Count: {}", *val))).await;
            Ok(())
        })
    }))
    .build()
    .register();
```

## Manifest / Capability / RuntimeConfig

插件可声明能力、运行时配置和资源清单：

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
MessageHandler::exact("id", vec!["/ping"], handler)
MessageHandler::prefix("id", vec!["/admin"], handler)
MessageHandler::keyword("id", vec!["delete"], handler)
MessageHandler::regex("id", r"^\d{11}$", handler)
MessageHandler::fallback("id", handler)
MessageHandler::new("id", RouteHint::Fallback, Some(rule), handler)
```

## SystemEvent 事件处理

除了响应聊天消息，插件还可以监听业务事件（下单、拍下、系统通知等）。这些事件来自 WebSocket 的非聊天推送，由 Adapter 的 `classify_event_type()` 自动分类后派发。

### EventHandler

```rust
pub type EventHandlerFunc = Arc<
    dyn Fn(Arc<SystemEvent>, Arc<dyn BaseAdapter>, Arc<Ctx>, Option<Arc<dyn Any + Send + Sync>>)
        -> EventHandlerFuture + Send + Sync,
>;
```

第 4 个参数是 `plugin_state`，proc macro 自动处理。

### SystemEvent 结构

```rust
pub struct SystemEvent {
    pub event_type: String,
    pub payload: Arc<serde_json::Value>,
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
QueueStrategy::DropNewest         // (默认)队列满时丢弃新事件
QueueStrategy::DropOldest(usize)  // 队列满时丢弃最旧事件，为新事件腾位
```

## 可观测指标

60 秒自动输出一次摘要。指标分为三层：

- **路由层**: `messages_received`, `exact_route_hits`, `unmatched_messages`, `handler_dispatches` ...
- **Handler 层**: `handler_started`, `handler_succeeded`, `handler_failed`, `handler_timed_out`
- **队列层**: `drop_newest_drops`, `drop_oldest_enqueues`, `queued_handler_succeeded` ...

Handler 内可通过 `ctx.telemetry()` 访问计数器。

## 依赖注入

```rust
// main.rs 启动时注入
let ctx = Arc::new(Ctx::new());
ctx.insert(my_db_pool);

// handler 中按类型取出
let pool = ctx.app_ctx().get::<PgPool>();
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
