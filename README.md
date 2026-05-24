# fish-bot

基于 kameo actor 框架的闲鱼聊天机器人。

## 为什么用 actor 模型？

聊天机器人需要同时处理多个会话——慢操作（API 调用、网络 I/O）不应阻塞快操作。Actor 模型让每个插件运行在独立的轻量级任务中，拥有隔离的状态、独立的并发控制和自动的 panic 恢复：

```
adapter → Bot (路由器) → PluginActor A ── 信号量 ── handler 任务
                          PluginActor B ── 信号量 ── handler 任务
                          PluginActor C ── 信号量 ── handler 任务
```

每个 actor 有独立的信号量限制并发数。插件 A 某个 handler 慢，只消耗 A 的许可池，B 和 C 完全不受影响。任何 handler 的 panic 被 actor 框架捕获，不会扩散。

## 架构

```
┌─────────────┐     ┌──────────────────────────────────────────────────┐
│  Adapter    │────▶│  Bot (路由器)                                    │
│  (WebSocket │     │                                                  │
│   + API)    │     │  exact_routes: HashMap<String, Vec<RouteTarget>> │  O(1)
│             │     │  prefix_routes:  Vec<(String, RouteTarget)>      │  O(n)
│  回调:      │     │  keyword_routes: Vec<(String, RouteTarget)>      │  O(n)
│  MessageEvent   │  │  fallback_routes: Vec<RouteTarget>               │  总是
│  SystemEvent│     │  event_routes: HashMap<String, Vec<RouteTarget>> │  按类型
└─────────────┘     └─────┬────────────────────────────────────────────┘
                          │ 分发到匹配的 handler
                          ▼
              ┌──────────────────────────┐
              │  PluginActor (每个插件)   │
              │  ┌─ 信号量               │
              │  └─ QueueStrategy        │
              └──────────────────────────┘
                          │ 启动 handler
                          ▼
                 handler 任务 — tokio::spawn
```

启动时，Bot 根据所有插件声明的 `RouteHint` 编译路由表：

- **精确匹配** → `HashMap` 查找，O(1)，零扫描
- **前缀/关键词** → 线性扫描注册的目标
- **正则/兜底** → 总是派发，PluginActor 自行检查规则
- **事件路由** → 按事件类型 `HashMap` 查找

大多数消息以常数时间找到目标，PluginActor 只收到自己能处理的事件。

## 快速开始

```bash
RUST_LOG=info cargo run -p fish-bot
```

首次运行无凭证时自动弹出终端二维码，用闲鱼 App 扫码登录。

```bash
# 减少依赖库的日志输出
RUST_LOG=info,reqwest=warn,tungstenite=warn cargo run -p fish-bot
```

## 写一个插件

两个 proc macro 完成全部定义：

```rust
use fish_plugin_sdk::prelude::*;

#[plugin(id = "greeter", name = "Greeter")]
#[derive(Default)]
struct Greeter;

#[plugin_handlers]
impl Greeter {
    #[command("/ping")]
    async fn ping(&self, ctx: Context) -> Result<()> {
        ctx.reply("pong!").await?;
        Ok(())
    }
}

register_plugin(Greeter::default());
```

### Handler 签名与状态语义

三种 receiver 形式，对应不同的并发保证：

```rust
// 无状态——不访问 struct，完全并发
#[command("/stats")]
async fn stats(ctx: Context) -> Result<()> { ... }

// 只读状态——允许并发读
#[command("/count")]
async fn read(&self, ctx: Context) -> Result<()> { ... }

// 可变状态——串行化写
#[command("/incr")]
async fn incr(&mut self, ctx: Context) -> Result<()> { ... }
```

actor 自动检测 receiver 类型并选择对应的锁：`&mut self` 获取写锁（串行），`&self` 获取读锁（并发）。
同一个 impl 块中混用不同签名也能正常工作。

### 状态即 struct 字段

插件的状态就是普通的 struct 字段——不需要包装类型，不需要手动 downcast：

```rust
#[plugin(id = "counter", name = "Counter")]
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

`&mut self` handler 在同一 actor 中串行执行，`&self` handler 可以并发执行。
内部使用 `tokio::sync::RwLock` 管理状态，不需要手写 `Mutex` 样板代码。

### 自定义初始化

`#[plugin]` 不会自动注入 `Default`——需要显式添加，或通过 `init` 参数指定：

```rust
#[plugin(id = "my_plugin", name = "My Plugin", init = "MyPlugin::new()")]
struct MyPlugin {
    count: u64,
}

impl MyPlugin {
    fn new() -> Self { Self { count: 42 } }
}
```

### 匹配模式

```rust
#[command("/ping")]                     // 精确匹配（默认）
#[command("/admin", kind = "prefix")]   // 前缀匹配
#[command(pattern = r"\d+", kind = "regex")]  // 正则匹配
#[command(fallback)]                    // 兜底，无匹配时执行

#[message(keyword = "最低多少钱")]       // 关键词匹配
```

### 事件处理

处理下单、售出等业务事件：

```rust
#[event("order_create")]
async fn on_order(&self, ctx: Context) -> Result<()> {
    tracing::info!("新订单: {:?}", ctx.payload()?);
    Ok(())
}
```

事件 handler 的 `Context` 处于 System 上下文——`ctx.reply()`、`ctx.text()`、`ctx.sender_id()` 会返回错误（它们只在消息上下文有意义）。
应使用 `ctx.event_type()` 和 `ctx.payload()`。

### Context API

```
消息上下文方法（#[command] / #[message] 中可用）：
  ctx.reply("hello").await?     — 回复消息
  ctx.sender_id()?              — 发送者用户 ID
  ctx.cid()?                    — 会话/频道 ID
  ctx.text()?                   — 消息纯文本
  ctx.event()?                  — 原始 MessageEvent

系统事件上下文方法（#[event] 中可用）：
  ctx.event_type()?             — 事件类型字符串
  ctx.payload()?                — 事件 JSON 载荷

通用方法：
  ctx.adapter()                 — 发送任意消息
  ctx.app_ctx()                 — 依赖注入容器
  ctx.telemetry()               — 可观测计数器
```

在系统 handler 中调用消息方法（反之亦然）会返回 `AppError::Internal`——不会 panic。

## PluginBuilder（免 macro 方式）

适合程序化构建插件，或 proc macro 不适用的场景：

```rust
use fish_plugin_sdk::prelude::*;
use std::sync::Arc;

PluginBuilder::new("echo", "Echo")
    .command("echo", "/echo", Arc::new(|cx: HandlerContext| {
        Box::pin(async move {
            cx.event.reply(MessageSegment::text("echo!")).await;
            Ok(())
        })
    }))
    .build()
    .register();
```

有状态版本，显式传入初始状态：

```rust
PluginBuilder::new("counter", "Counter")
    .state(0u64)
    .command("incr", "/incr", Arc::new(|cx: HandlerContext| {
        Box::pin(async move {
            let state = cx.plugin_state.clone().expect("有状态插件");
            let lock = state.downcast::<parking_lot::RwLock<u64>>().expect("类型错误");
            let mut val = lock.write();
            *val += 1;
            cx.event.reply(MessageSegment::text(format!("Count: {}", *val))).await;
            Ok(())
        })
    }))
    .build()
    .register();
```

### 能力声明与运行时配置

```rust
PluginBuilder::new("my_plugin", "My Plugin")
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
| `ReadAppContext` | 可访问 Ctx 容器 |

## 规则系统

Rule 是 `MessageEvent` 的可组合谓词：

```rust
use fish_core::rule::*;

// 组合子
is_startswith("/admin").and(&is_keywords("delete"))
is_fullmatch(["/help", "/h", "帮助"])
is_regex(r"^\d{11}$").or(&is_fullmatch(["/phone"]))

// 自定义
Rule::new(|event: &MessageEvent| event.has_image())
```

`RouteHint` 告诉 Bot 如何索引 handler；`Rule` 是 PluginActor 中的实际匹配器。
两者应一致但目的不同：

- `RouteHint` 用于路由（性能）
- `Rule` 用于匹配（语义）

## 系统事件

闲鱼通过 WebSocket 推送业务事件（非聊天消息）。Adapter 自动分类 `redReminder` 载荷：

| redReminder | 映射事件 | 含义 |
|---|---|---|
| `1` | `order_create` | 买家下单 |
| `2` | `order_closed` | 交易关闭 |
| `3` | `item_purchased` | 商品售出 |

事件通过 Bot 的 `event_routes` 派发，由 `#[event("order_create")]` handler 接收。

## 消息协议

消息解密链路：

```
base64 → msgpack → JSON（对嵌套字段递归）
```

1. base64 解码
2. 用 `rmp-serde` 反序列化为 JSON Value
3. msgpack 失败时退化为 JSON 直接解析
4. 对已知嵌套字段递归执行解密

过滤器丢弃以下消息：
- 输入状态指示（正在输入）
- `needPush=false` 的系统推送
- 空或格式错误的消息

## 队列策略

插件达到并发上限时的处理策略：

| 策略 | 行为 |
|---|---|
| `DropNewest`（默认） | 直接丢弃新事件 |
| `DropOldest(n)` | 有界队列，丢弃最旧事件，追加最新事件 |

防止慢插件堆积无限积压。

## 可观测性

18 个原子计数器，每 60 秒自动输出摘要：

| 层 | 计数器 |
|---|---|
| 路由 | `messages_received`, `exact_route_hits`, `unmatched_messages`, `handler_dispatches` |
| Handler | `handler_started`, `handler_succeeded`, `handler_failed`, `handler_timed_out` |
| 队列 | `drop_newest_drops`, `drop_oldest_enqueues`, `drop_oldest_oldest_discards`, `queued_handler_succeeded/failed/timed_out` |

handler 中访问：`ctx.telemetry().handler_started.fetch_add(1, ...)`。

## 依赖注入

```rust
// 启动时注入
let ctx = Arc::new(Ctx::new());
ctx.insert(my_db_pool);

// 任意 handler 中取出
let pool = ctx.app_ctx().get::<PgPool>();
```

`Ctx` 是基于 `TypeId` 的容器，使用 `parking_lot::RwLock`——不需要静态生命周期。

## 错误处理

`AppError` 是基于 `snafu` 的错误枚举，每类错误有对应的构造函数：

```rust
pub enum AppError {
    Http,       // HTTP 请求失败
    Ws,         // WebSocket 错误
    Json,       // serde_json 错误（自动 From 转换）
    Base64,     // base64 解码错误（自动 From 转换）
    Auth,       // 认证失败
    Protocol,   // 协议层错误
    Internal,   // 内部逻辑错误（上下文不匹配、不变性违反）
}
```

生产代码路径零 unwrap。`parking_lot` 锁不会中毒。`serde_json` 和 `base64` 错误通过 `?` 自动转换。

## Crate 结构

```
fish-core          核心数据类型（MessageEvent, SystemEvent, Rule, AppError, Ctx, Telemetry）
fish-adapter       平台适配层（WebSocket, API, 认证, 协议, MTOP 签名）
fish-plugin        插件 trait、MessageHandler、EventHandler、注册表
fish-runtime       Actor 运行时（PluginActor、派发、队列策略）
fish-plugin-sdk    统一 SDK 入口：re-export、Context、Builder、prelude
fish-plugin-macros Proc macro：#[plugin] + #[plugin_handlers]
fish-bot           二进制入口、Bot 路由器、组装启动
```

依赖方向（从上到下）：

```
fish-bot → fish-runtime → fish-plugin-sdk → fish-plugin → fish-adapter → fish-core
fish-bot → fish-adapter → fish-core
fish-plugin-macros（独立 proc-macro crate）
```

所有依赖为单向，无循环。

## License

MIT OR Apache-2.0
