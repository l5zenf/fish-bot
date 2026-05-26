# fish-bot



`fish-bot` 是一个面向 `fish` 消息场景的 Rust 运行时。

![](./assets/bkg.png)

它只解决一个问题：

**把平台接入和业务逻辑拆开。**

## 免责声明


> 由于本项目的特殊性与敏感性，不保证代码的长期稳定可用。
> 本项目仅供个人学习与安全研究使用。请在**下载后 24 小时内删除**，不得用于任何商业、非法、灰黑产或侵犯他人权益的场景。
> 本项目随时可能停止维护或直接下线；由使用本项目产生的一切风险与后果，均由使用者**自行承担**。



如果你直接做机器人，代码通常会很快混成一层：

- 一部分在处理 `fish` 的登录、认证、连接和发消息
- 一部分在处理命令、规则、回复和状态
- 一部分在处理你自己的业务逻辑

这个仓库的目标是把这三件事拆开，让它们通过稳定边界协作。

## 它不做什么

这个仓库不是：

- 完整业务系统
- 平台 SDK 大全
- 已经封装好全部业务接口的成品

它只提供一个可扩展底座：

- `adapter` 负责接平台
- `runtime` 负责调度
- `plugin` 负责业务

## 第一性原理

从运行时视角看，系统里只有一条链路：

```text
platform event
  -> adapter
  -> runtime
  -> plugin
  -> handler
```

职责边界如下：

- `BaseAdapter`
  负责接收外部事件、发送消息、向运行时暴露底层基础能力。
- `RuntimeHost`
  负责把 adapter、plugins、`Ctx`、`Telemetry` 组装起来并启动。
- `Plugin`
  负责业务处理。
- `Ctx`
  负责依赖注入和共享上下文。

运行时本身不关心你处理的是什么业务，也不关心 adapter 背后接的是哪个平台。

## 仓库结构

这是一个 workspace，不是单个二进制程序：

```text
crates/
  fish-core
  fish-runtime
  fish-rt-adapter
  fish-plugin-macros

examples/
  quickstart-simple
  quickstart-custom
```

各 crate 的职责是：

- `fish-core`
  定义稳定边界：`BaseAdapter`、消息模型、事件模型、`Ctx`、错误类型。
- `fish-runtime`
  提供运行时编排：插件调度、上下文挂载、telemetry、actor-first 支持。
- `fish-rt-adapter`
  提供默认的 `FishWebSocketAdapter`，以及 fish 平台相关的基础能力。
- `fish-plugin-macros`
  提供 `#[plugin]`、`#[message]`、`#[event]` 等宏。

依赖方向保持单向：

```text
fish-runtime -> fish-core
fish-rt-adapter -> fish-core + fish-runtime
fish-plugin-macros -> fish-runtime
```

## 什么时候适合用它

适合：

- 你想做一个 `fish` 消息机器人，但不想把接入、调度、业务揉在一起
- 你想保留现成的 runtime，只替换 adapter
- 你想把平台认证和连接复用给上层业务

不适合：

- 你只想要一个已经封装好全部业务 API 的 SDK
- 你要的是完整成品，而不是可继续演进的底座

## 快速开始

先确认本机有 Rust：

```bash
cargo --version
```

运行最简单的宏插件示例：

```bash
cargo run -p fish-example-quickstart-simple
```

运行 actor-first 示例：

```bash
cargo run -p fish-example-quickstart-custom
```

如果你已经拿到浏览器里的原始 Cookie header，也可以在启动前导入：

```bash
cargo run -p fish-example-quickstart-simple -- --cookies "cookie2=...; unb=...; _m_h5_tk=..."
```

或者：

```bash
cargo run -p fish-example-quickstart-custom -- --cookies "cookie2=...; unb=...; _m_h5_tk=..."
```

导入后，认证信息会持久化到 `data/fish_auth.json`。

## 最小宿主

最小宿主只需要四样东西：

- 一个 adapter
- 一组 plugins
- 一个 `Ctx`
- 一个 `Telemetry`

```rust
use std::sync::Arc;

use fish_rt_adapter::plugin;
use fish_rt_adapter::prelude::*;
use fish_rt_adapter::{BaseAdapter, FishWebSocketAdapter, RuntimeHost, Telemetry};

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

    let host = RuntimeHost::new(
        adapter,
        plugins,
        Arc::new(Ctx::new()),
        Arc::new(Telemetry::new()),
    );

    host.run().await
}
```

如果你只想快速理解这个仓库，这段代码就是核心。

## 写插件

这个仓库支持两种主要插件写法。

### 1. 宏插件

适合“收到消息 -> 判断 -> 回复”这类直接逻辑。

```rust
use fish_rt_adapter::plugin;
use fish_rt_adapter::prelude::*;

pub struct EchoPlugin;

#[plugin]
impl EchoPlugin {
    #[message("/ping")]
    async fn ping(&self, ctx: MessageContext) -> Result<()> {
        ctx.reply("pong").await?;
        Ok(())
    }

    #[message(keyword = "fish")]
    async fn on_keyword(&self, ctx: MessageContext) -> Result<()> {
        ctx.reply(format!("keyword hit: {}", ctx.text())).await?;
        Ok(())
    }
}
```

### 2. Actor-first 插件

适合插件内部有明确状态模型，或者你想自己控制 actor 邮箱和消息类型的场景。

```rust
use fish_rt_adapter::ActorPluginBuilder;
use fish_rt_adapter::prelude::*;
use kameo::Actor;
use kameo::message::{Context, Message};

#[derive(Actor)]
struct CounterActor {
    seen: u64,
}

fn build_plugin() -> ActorPluginBuilder<CounterActor> {
    ActorPluginBuilder::new(|| CounterActor { seen: 0 })
        .id("quickstart_custom_actor")
        .name("QuickstartCustomActor")
        .bounded_mailbox(64)
        .on_message("/ping", Ping)
}

struct Ping(MessageContext);

impl Message<Ping> for CounterActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: Ping, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.seen += 1;
        msg.0.reply(format!("actor pong #{}", self.seen)).await
    }
}
```

## 运行时暴露什么能力

运行时不会把业务逻辑做进 `runtime`，但 adapter 可以把基础能力注入到 `Ctx`。

当前默认的 fish adapter 会注入：

- `FishHttpClient`

它实现了：

- `ClientProvider`

所以业务层既可以拿一个共享的 `reqwest::Client`，也可以借用底层 client：

```rust
use fish_rt_adapter::{ClientProvider, FishHttpClient};

let fish = ctx
    .app_ctx()
    .get::<FishHttpClient>()
    .ok_or_else(|| AppError::internal("fish http client missing"))?;

let client = fish.client();
let client_ref = fish.client_ref();
```

这层能力的定位是：

- 复用 adapter 已经初始化好的底层 HTTP client
- 不把认证管理细节直接暴露给业务
- 不把业务 API 做死在 runtime 里

如果你要商品详情、订单详情、会话列表这类业务 client，建议在上层自己封装，底层依赖这个 `FishHttpClient` 即可。

## 自定义 adapter

如果你不想使用默认的 fish adapter，只需要实现 `BaseAdapter`：

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
        println!("send -> target={target_id}, cid={cid:?}, payload={}", message.summary());
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

如果你还想向业务层暴露底层能力，也可以在 adapter 里实现 `register_context(...)`，把自己的依赖注入到 `Ctx`。

## Cookie 和认证文件

仓库支持把浏览器抓到的原始 Cookie header 直接导入：

```rust
use fish_rt_adapter::import_browser_cookies;

let report = import_browser_cookies(raw_cookie_header).await?;
println!("imported {} cookies into {}", report.imported, report.path.display());
```

默认认证文件路径是 `data/fish_auth.json`。

这个文件：

- 用来持久化 fish 认证信息
- 已经在 `.gitignore` 中忽略
- 不应该提交到仓库

## 常用导出

`fish-rt-adapter` 当前最常用的入口有：

- `FishWebSocketAdapter`
- `RuntimeHost`
- `ClientProvider`
- `FishHttpClient`
- `ActorPluginBuilder`
- `import_browser_cookies`
- `plugin`
- `prelude::*`

## 先看哪里

如果你第一次读这个仓库，建议按这个顺序看：

1. [examples/quickstart-simple/src/app/bootstrap.rs](/Users/xlh/Downloads/fish-bot/examples/quickstart-simple/src/app/bootstrap.rs)
2. [examples/quickstart-simple/src/app/plugin.rs](/Users/xlh/Downloads/fish-bot/examples/quickstart-simple/src/app/plugin.rs)
3. [examples/quickstart-custom/src/app/bootstrap.rs](/Users/xlh/Downloads/fish-bot/examples/quickstart-custom/src/app/bootstrap.rs)
4. [examples/quickstart-custom/src/app/plugin.rs](/Users/xlh/Downloads/fish-bot/examples/quickstart-custom/src/app/plugin.rs)
5. [crates/fish-rt-adapter/src/lib.rs](/Users/xlh/Downloads/fish-bot/crates/fish-rt-adapter/src/lib.rs)

先看 example，再回来看 crate，理解会最快。

## 测试

跑全部测试：

```bash
cargo test
```

只跑 adapter：

```bash
cargo test -p fish-rt-adapter -- --nocapture
```
