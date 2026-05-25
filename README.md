# fish-bot

![](./assets/bkg.png)

一个围绕闲鱼消息场景构建的 Rust 插件运行时。

现在的设计目标很明确：

- `fish-core` 定义稳定抽象和通用模型
- `fish-runtime` 提供运行时编排能力，并内置默认的闲鱼适配实现
- `fish-plugin-macros` 提供插件声明宏
- 宿主只是组装 `adapter + plugins + context`，不和运行时内部实现耦合

这意味着你可以把 `fish-runtime` 嵌进任何宿主里使用，不管入口是 CLI、`axum`、`pyo3`，还是你自己的进程管理框架。仓库里的 example 只是接线示例，不是唯一使用方式。

## 当前结构

```text
crates/
  fish-core           稳定抽象：BaseAdapter / AdapterEventSink / 事件 / 消息 / Rule / Ctx
  fish-runtime        运行时编排：RuntimeHost / PluginActor / ActorPluginBuilder / 默认 Fish 适配器
  fish-plugin-macros  #[plugin] / #[message] / #[event]

examples/
  quickstart          离线最小可运行示例
  fish-app            真实闲鱼宿主骨架
```

依赖方向保持单向：

```text
fish-runtime -> fish-core
fish-plugin-macros -> fish-runtime
```

## 核心心智模型

运行时只做一件事：把外部平台事件安全、可控地分发给插件。

```text
BaseAdapter
  -> RuntimeHost
     -> PluginActor
        -> handler
```

职责划分：

- `BaseAdapter`
  - 负责接入外部平台
  - 把消息事件和系统事件推给 runtime
  - 提供发送消息能力
- `RuntimeHost`
  - 负责把 adapter、plugins、共享上下文组装起来
  - 负责基于 `RouteHint` 做消息预路由
  - 作为标准启动入口
- `PluginActor`
  - 为单个插件隔离状态、并发和超时控制
  - 执行真正的 handler

这个边界的重点是：宿主只依赖 trait 和公开 API，不需要知道 runtime 内部怎么调度。

## 为什么用 actor

聊天类业务天然是高并发、多会话、慢操作和快操作混在一起。actor 模型比较适合这个场景：

- 插件状态天然隔离
- 一个插件变慢不会拖垮其他插件
- `&self` 和 `&mut self` handler 能自然表达并发语义
- panic 和超时可以限制在单个插件 actor 内部

这比把所有插件都挂在一堆全局变量上更容易维护，也更容易替换宿主实现。

## 快速开始

仓库里有两个 example 项目：

- `examples/quickstart`
  - 离线运行
  - 用本地 `LocalAdapter` 模拟事件下推
  - 用来理解最小接线方式
- `examples/fish-app`
  - 使用 `fish-runtime` 内置的 `FishWebSocketAdapter`
  - 作为真实闲鱼宿主骨架

先跑离线例子：

```bash
cargo run -p fish-example-quickstart
```

再跑真实闲鱼宿主：

```bash
cargo run -p fish-example-fish-app
```

运行 `fish-app` 前，建议准备本地认证信息：

- `FISH_AUTH_JSON`
- 或 `FISH_DATA_DIR/fish_auth.json`

## 宿主如何启动 runtime

标准启动方式就是把插件列表和 adapter 交给 `RuntimeHost`：

```rust
use std::sync::Arc;

use fish_runtime::prelude::*;
use fish_runtime::{plugin, FishWebSocketAdapter, RuntimeHost};

struct EchoPlugin;

#[plugin]
impl EchoPlugin {
    #[message("/ping")]
    async fn ping(&self, ctx: MessageContext) -> Result<()> {
        ctx.reply("pong").await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let adapter: Arc<dyn BaseAdapter> = Arc::new(FishWebSocketAdapter::new());
    let plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(EchoPlugin)];

    RuntimeHost::with_plugins(adapter, plugins).run().await
}
```

如果你需要注入自己的共享依赖，也可以显式构造：

```rust
use std::sync::Arc;

use fish_runtime::prelude::*;
use fish_runtime::RuntimeHost;

let adapter: Arc<dyn BaseAdapter> = Arc::new(MyAdapter::new());
let plugins: Vec<Arc<dyn Plugin>> = vec![Arc::new(MyPlugin::new())];

let ctx = Arc::new(Ctx::new());
ctx.insert(MyDatabasePool::new());

let telemetry = Arc::new(Telemetry::new());

RuntimeHost::new(adapter, plugins, ctx, telemetry).run().await?;
```

## 如果不用默认闲鱼实现

`fish-runtime` 内置了默认的闲鱼 adapter，但 runtime 本身并不绑定闲鱼。

你只要实现 `fish-core::BaseAdapter`，就能把 runtime 接到任何外部系统上：

```rust
use async_trait::async_trait;
use std::sync::Arc;

use fish_core::{AdapterEventSink, BaseAdapter};
use fish_core::error::Result;
use fish_core::event::MessageEvent;
use fish_core::message::MessageChain;

struct MyAdapter;

#[async_trait]
impl BaseAdapter for MyAdapter {
    async fn send(&self, target_id: &str, message: &MessageChain, cid: Option<&str>) -> Result<()> {
        println!("send -> target={target_id}, cid={:?}, payload={}", cid, message.summary());
        Ok(())
    }

    async fn run(&self, sink: Arc<dyn AdapterEventSink>) -> Result<()> {
        sink.handle_message(MessageEvent::new(
            "demo-cid".into(),
            "demo-user".into(),
            "Demo User".into(),
            "/ping".into(),
            serde_json::json!({ "source": "custom-adapter" }),
        ))
        .await?;

        Ok(())
    }
}
```

这里的关键点不是“继承闲鱼实现”，而是“遵守稳定 trait 边界”。

## 写插件

推荐的插件开发方式是 `struct + #[plugin] impl`：

```rust
use fish_runtime::prelude::*;
use fish_runtime::plugin;

struct Greeter {
    count: u64,
}

#[plugin(Self { count: 0 })]
impl Greeter {
    #[message("/ping")]
    async fn ping(&self, ctx: MessageContext) -> Result<()> {
        ctx.reply("pong").await?;
        Ok(())
    }

    #[message(keyword = "fish")]
    async fn on_keyword(&mut self, ctx: MessageContext) -> Result<()> {
        self.count += 1;
        ctx.reply(format!("keyword hit: {}, count={}", ctx.text(), self.count))
            .await?;
        Ok(())
    }

    #[event("order_create")]
    async fn on_order(&self, ctx: EventContext) -> Result<()> {
        tracing::info!("event={}, payload={}", ctx.event_type(), ctx.payload());
        Ok(())
    }
}
```

### Handler 签名语义

宏模式现在只保留**状态化插件**这一条路，handler 必须使用 receiver：

```rust
#[message("/read")]
async fn read(&self, ctx: MessageContext) -> Result<()> { ... }

#[message("/write")]
async fn write(&mut self, ctx: MessageContext) -> Result<()> { ... }
```

- `&self`
  - 读状态
  - 允许并发执行
- `&mut self`
  - 写状态
  - 在单插件 actor 内串行化

如果你想写真正的 actor-first 插件，不走宏，直接使用 `ActorPluginBuilder`。

这也是当前推荐的“功能内聚，一个插件 struct 承载自己的状态和行为”的用法。

另外，插件声明现在分成两层：

- `#[plugin] impl Type`
  - 挂在插件的 `impl` 上
  - 负责状态初始化和 handler 注册
  - `name` 默认等于 struct 名
  - `id` 默认等于 struct 名的 snake_case
  - 对 unit struct 默认使用 `Self` 初始化
  - 有状态 struct 时直接写 Rust 表达式，例如 `#[plugin(Self { count: 0 })]`

### 路由方式

```rust
#[message("/ping")]
#[message("/admin", kind = "prefix")]
#[message(pattern = r"^\d+$", kind = "regex")]
#[message(fallback)]

#[message(keyword = "最低多少钱")]
#[event("order_create")]
```

消息侧现在推荐只记一个属性：`#[message(...)]`。  
`command` 仍可作为兼容别名使用，但不再是推荐 API。

推荐写法：

- 精确匹配：`#[message("/ping")]`
- 关键词匹配：`#[message(keyword = "你好")]`
- 前缀匹配：`#[message("/admin", kind = "prefix")]`
- 正则匹配：`#[message(pattern = r"^\\d+$", kind = "regex")]`
- 兜底：`#[message(fallback)]`

`RuntimeHost` 启动后，会根据 `RouteHint` 建索引，尽量把事件只派发给可能命中的插件。

## Context API

现在对插件作者暴露的是两个明确上下文，而不是一个“双语义 Context”：

- `MessageContext`
  - 用在 `#[message]`
  - 常用方法：`reply(...)`、`text()`、`sender_id()`、`cid()`、`event()`
- `EventContext`
  - 用在 `#[event]`
  - 常用方法：`event_type()`、`payload()`、`event()`

两个上下文都提供：

- `adapter()`
- `app_ctx()`
- `telemetry()`

这样插件作者不需要再记“哪些方法只有在某种上下文里才能调用”，类型本身就表达了能力边界。

## 状态模型

当前项目有两种状态使用方式。

### 方式一：状态就是插件 struct 字段

这是默认推荐方式，最符合“高内聚”：

```rust
struct Counter {
    value: u64,
}

#[plugin(Self { value: 0 })]
impl Counter {
    #[message("/incr")]
    async fn incr(&mut self, ctx: MessageContext) -> Result<()> {
        self.value += 1;
        ctx.reply(format!("value={}", self.value)).await?;
        Ok(())
    }
}
```

### 方式二：用 `ActorPluginBuilder` 走 actor-first 范式

适合你希望插件就是 actor、本身有 typed message、有 `ask` / `tell` 心智的场景：

```rust
use fish_runtime::prelude::*;
use kameo::Actor;
use kameo::message::{Context, Message};

#[derive(Actor)]
struct CounterActor {
    value: u64,
}

struct Incr(MessageContext);
struct CurrentValue;

impl Message<Incr> for CounterActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: Incr, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.value += 1;
        msg.0.reply(format!("value={}", self.value)).await
    }
}

impl Message<CurrentValue> for CounterActor {
    type Reply = u64;

    async fn handle(
        &mut self,
        _msg: CurrentValue,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.value
    }
}

let plugin = ActorPluginBuilder::new("counter", "Counter", || CounterActor { value: 0 })
    .bounded_mailbox(128)
    .on_message("incr", "/incr", Incr);
```

这条路的重点不是“再包一层 handler 闭包”，而是：

- 你定义的是 actor 本身
- 路由只负责把 `MessageContext` / `EventContext` 映射成 typed message
- 插件内部状态在 actor 里，不在 `RwLock<T>` 里
- builder 本身就是最终插件定义，`.build()` 只是可选收口
- mailbox 只是 builder 上的一个配置点，不再额外引入公开类型
- 需要时可以直接拿 `actor_ref()` 做 `ask` / `tell`

## ActorPluginBuilder

`ActorPluginBuilder` 的价值在于：

- 让插件作者直接写 `kameo` actor 和 typed message
- 让 runtime 只负责 route -> actor message 的转发
- 保留运行时的 metadata、telemetry、timeout、queue 配置
- 不需要为了 actor 插件再退回到“注册一组闭包”

## 共享依赖注入

`Ctx` 是一个按类型存取的共享容器，用来承载宿主注入的依赖：

```rust
let ctx = Arc::new(Ctx::new());
ctx.insert(MyDatabasePool::new());
ctx.insert(MyConfig::from_env()?);
```

在 handler 中读取：

```rust
let pool = ctx.app_ctx().get::<MyDatabasePool>();
```

这部分是宿主和业务之间的桥，不应该退化成一组难以维护的全局变量。

## 运行时行为

运行时默认做了几件事：

- 基于 `RouteHint` 的消息预路由
- 每个插件独立 actor、独立并发限制
- handler 超时控制
- 队列策略控制
- telemetry 统计

队列策略目前支持：

- `QueueStrategy::DropNewest`
- `QueueStrategy::DropOldest(n)`

如果某个插件很慢，它只会影响自己的 actor，不会把整个 runtime 拖成串行系统。

## 示例项目

### `examples/quickstart`

用途：

- 理解最小宿主接线
- 看 `BaseAdapter -> RuntimeHost -> Plugin` 的完整流转
- 离线验证插件行为

入口文件：

- [examples/quickstart/src/app/bootstrap.rs](/Users/xlh/Downloads/fish-bot/examples/quickstart/src/app/bootstrap.rs)
- [examples/quickstart/src/app/local_adapter.rs](/Users/xlh/Downloads/fish-bot/examples/quickstart/src/app/local_adapter.rs)
- [examples/quickstart/src/app/plugin.rs](/Users/xlh/Downloads/fish-bot/examples/quickstart/src/app/plugin.rs)

### `examples/fish-app`

用途：

- 作为真实闲鱼宿主骨架
- 演示如何直接使用 `FishWebSocketAdapter`
- 给后续业务系统一个干净的接入起点

入口文件：

- [examples/fish-app/src/app/bootstrap.rs](/Users/xlh/Downloads/fish-bot/examples/fish-app/src/app/bootstrap.rs)
- [examples/fish-app/src/app/plugin.rs](/Users/xlh/Downloads/fish-bot/examples/fish-app/src/app/plugin.rs)

## 项目原则

这次重构后的方向可以概括成四句话：

- trait 放在稳定边界上
- 运行时实现收敛在 `fish-runtime`
- 插件能力按功能内聚，不按想象中的扩展点过度拆 crate
- 宿主尽量薄，只负责接线和业务编排

如果未来要接其他宿主，优先扩展 adapter 和 bootstrap，不要把 runtime 重新拆碎。

## License

MIT OR Apache-2.0
