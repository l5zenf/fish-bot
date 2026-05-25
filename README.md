# fish-bot

![](./assets/bkg.png)

`fish-bot` 是一个面向 fish 消息场景的 Rust 插件运行时。

它解决的是同一类问题：把“平台接入”和“业务插件”拆开。你可以直接用仓库自带的 `FishWebSocketAdapter` 跑起来，也可以保留 runtime，只换掉 adapter，接到你自己的消息来源上。

## 这是什么

这个仓库不是单个可执行程序，而是一个 workspace：

- `fish-core`
  定义稳定边界：`BaseAdapter`、`AdapterEventSink`、消息模型、事件模型、规则、共享上下文。
- `fish-runtime`
  提供运行时编排：加载插件、分发消息、挂载上下文和 telemetry。
- `fish-rt-adapter`
  提供默认的 `FishWebSocketAdapter`，并统一对外 re-export 常用运行时 API。
- `fish-plugin-macros`
  提供 `#[plugin]`、`#[message]`、`#[event]` 等插件声明宏。

仓库里还带了两个可以直接运行的示例：

- `examples/quickstart-simple`
  演示宏插件写法。
- `examples/quickstart-custom`
  演示 actor-first 插件写法。

## 适合什么场景

这个项目适合下面两类需求：

- 你想快速做一个 fish 消息机器人，但不想把接入层、调度层、业务层全写在一起。
- 你想复用一套消息运行时，只把 fish adapter 当成默认实现。

如果你要的是“一个已经做完全部业务流程的成品系统”，这里不是那个方向。
如果你要的是“一个边界清晰、可以继续扩展的底座”，这个仓库就是干这个的。

## 仓库结构

```text
crates/
  fish-core
  fish-runtime
  fish-rt-adapter
  fish-plugin-macros

examples/
  quickstart-simple
  quickstart-custom

tests/
  adapter_test.rs
  protocol_test.rs
  model_test.rs
  fish_rt_adapter_api_test.rs
```

依赖方向保持单向：

```text
fish-runtime -> fish-core
fish-rt-adapter -> fish-core + fish-runtime
fish-plugin-macros -> fish-runtime
```

## 核心心智模型

运行时只做一件事：把外部消息交给插件。

```text
BaseAdapter
  -> RuntimeHost
     -> Plugin
        -> handler
```

职责拆分如下：

- `BaseAdapter`
  负责接入外部平台，以及发送消息。
- `RuntimeHost`
  负责把 adapter、plugins、`Ctx`、`Telemetry` 组装起来并启动。
- `Plugin`
  负责业务处理。

你可以把它理解成一条非常简单的链路：

1. adapter 收到平台消息
2. runtime 把消息路由到插件
3. 插件决定是否回复、记录状态或触发别的逻辑

## 快速开始

先确认本机有 Rust 工具链：

```bash
cargo --version
```

然后直接运行示例。

宏插件示例：

```bash
cargo run -p fish-example-quickstart-simple
```

actor-first 示例：

```bash
cargo run -p fish-example-quickstart-custom
```

如果你已经拿到了浏览器里的原始 Cookie header，也可以在启动前导入：

```bash
cargo run -p fish-example-quickstart-simple -- --cookies "cookie2=...; unb=...; _m_h5_tk=..."
```

或者：

```bash
cargo run -p fish-example-quickstart-custom -- --cookies "cookie2=...; unb=...; _m_h5_tk=..."
```

导入后，认证信息会持久化到 `data/fish_auth.json`。

## Cookie 和认证文件

仓库支持把浏览器抓到的原始 Cookie header 直接导入：

```rust
use fish_rt_adapter::import_browser_cookies;

let report = import_browser_cookies(raw_cookie_header).await?;
println!("imported {} cookies into {}", report.imported, report.path.display());
```

解析时会自动过滤浏览器附带的 Cookie 属性，例如：

- `Max-Age`
- `Expires`
- `Path`
- `Domain`
- `SameSite`
- `Secure`
- `HttpOnly`

默认认证文件路径是 `data/fish_auth.json`。这个文件已经在 `.gitignore` 中忽略，不应该提交。

## 最小宿主示例

最常见的启动方式就是把 adapter 和 plugins 交给 `RuntimeHost`：

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

## 写插件

这个仓库有两条主要插件开发路径。

### 宏插件

这是最直接的写法，也是 `examples/quickstart-simple` 用的方式：

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

适合场景：

- 插件逻辑比较直白
- 你更关心“处理消息”而不是“控制 actor 细节”
- 你想用声明式方式快速起一个插件

### Actor-first 插件

如果你希望显式控制 actor 状态和邮箱，可以使用 `ActorPluginBuilder`。这是 `examples/quickstart-custom` 的方式：

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
        .on_keyword("runtime", KeywordHit)
}

struct Ping(MessageContext);
struct KeywordHit(MessageContext);

impl Message<Ping> for CounterActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: Ping, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.seen += 1;
        msg.0.reply(format!("actor pong #{}", self.seen)).await
    }
}
```

适合场景：

- 你需要更明确的 actor 状态模型
- 你想自己控制邮箱大小、消息类型和 actor 生命周期
- 插件内部逻辑比简单 handler 更复杂

## 自定义 adapter

如果你不想使用默认的 fish adapter，只要实现 `BaseAdapter`：

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

只要 trait 边界一致，runtime 不关心你背后接的是哪个平台。

## 常用导出

`fish-rt-adapter` 当前对外导出的常用入口有：

- `FishWebSocketAdapter`
- `import_browser_cookies`
- `RuntimeHost`
- `ActorPluginBuilder`
- `plugin`
- `prelude::*`

常见导入方式：

```rust
use fish_rt_adapter::prelude::*;
use fish_rt_adapter::{FishWebSocketAdapter, RuntimeHost, Telemetry, plugin};
```

## 测试

跑全部测试：

```bash
cargo test
```

只跑 adapter：

```bash
cargo test -p fish-rt-adapter -- --nocapture
```

只跑 facade 集成测试：

```bash
cargo test --test fish_rt_adapter_api_test -- --nocapture
```

## 从哪里看代码

如果你想快速建立上下文，优先看这些文件：

- [examples/quickstart-simple/src/app/bootstrap.rs](/Users/xlh/Downloads/fish-bot/examples/quickstart-simple/src/app/bootstrap.rs)
- [examples/quickstart-simple/src/app/plugin.rs](/Users/xlh/Downloads/fish-bot/examples/quickstart-simple/src/app/plugin.rs)
- [examples/quickstart-custom/src/app/bootstrap.rs](/Users/xlh/Downloads/fish-bot/examples/quickstart-custom/src/app/bootstrap.rs)
- [examples/quickstart-custom/src/app/plugin.rs](/Users/xlh/Downloads/fish-bot/examples/quickstart-custom/src/app/plugin.rs)
- [crates/fish-rt-adapter/src/lib.rs](/Users/xlh/Downloads/fish-bot/crates/fish-rt-adapter/src/lib.rs)

先看 examples，再回头看 crate，会更容易理解这个仓库的边界。
