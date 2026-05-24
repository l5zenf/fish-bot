# fish-bot

A kameo actor-based chat bot for 闲鱼 (Xianyu / Alibaba's second-hand marketplace).

## Why actor-based?

A chat bot handles multiple conversations concurrently — slow operations (API calls, network I/O)
shouldn't block fast ones. The actor model solves this by giving each plugin its own lightweight
task (a kameo actor) with isolated state, independent concurrency control, and automatic panic recovery.

```
adapter → Bot (router) → PluginActor A ── semaphore ── handler tasks
                          PluginActor B ── semaphore ── handler tasks
                          PluginActor C ── semaphore ── handler tasks
```

Each actor has its own semaphore-bounded concurrency. If plugin A has a slow handler, only A's
permit pool is affected — B and C run unaffected. A panic in any handler is caught by the actor
framework and doesn't propagate.

## Architecture

```
┌─────────────┐     ┌──────────────────────────────────────────────────┐
│  Adapter    │────▶│  Bot (router)                                    │
│  (WebSocket │     │                                                  │
│   + API)    │     │  exact_routes: HashMap<String, Vec<RouteTarget>> │  O(1)
│             │     │  prefix_routes:  Vec<(String, RouteTarget)>      │  O(n)
│  callbacks: │     │  keyword_routes: Vec<(String, RouteTarget)>      │  O(n)
│  MessageEvent  │  │  fallback_routes: Vec<RouteTarget>               │  always
│  SystemEvent│     │  event_routes: HashMap<String, Vec<RouteTarget>> │  by type
└─────────────┘     └─────┬────────────────────────────────────────────┘
                          │ fan-out to matched handlers
                          ▼
              ┌──────────────────────────┐
              │  PluginActor (per plugin) │
              │  ┌─ permit semaphore      │
              │  └─ QueueStrategy         │
              └──────────────────────────┘
                          │ spawn per handler
                          ▼
                 handler task — tokio::spawn
```

At startup, Bot compiles a routing table from every plugin's declared `RouteHint`.
- **Exact** routes → `HashMap` lookup, O(1), zero scanning
- **Prefix** / **Keyword** routes → linear scan over registered targets
- **Regex** / **Fallback** → always dispatched; the `PluginActor` checks the rule
- **Event** routes → `HashMap` by event type

This means most messages find their target in constant time, and PluginActors receive
only events they can actually handle.

## Quick start

```bash
RUST_LOG=info cargo run -p fish-bot
```

First run: no credentials → terminal QR code appears → scan with 闲鱼 app to log in.

```bash
# Quiet noisy dependencies
RUST_LOG=info,reqwest=warn,tungstenite=warn cargo run -p fish-bot
```

## Writing a plugin

Two proc macros define everything:

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

### Handler signatures and state semantics

Three receiver forms, each with different concurrency guarantees:

```rust
// Stateless — no struct access, full concurrency
#[command("/stats")]
async fn stats(ctx: Context) -> Result<()> { ... }

// Read-only state access — concurrent reads allowed
#[command("/count")]
async fn read(&self, ctx: Context) -> Result<()> { ... }

// Mutable state access — serialized writes
#[command("/incr")]
async fn incr(&mut self, ctx: Context) -> Result<()> { ... }
```

The actor framework detects the receiver kind and selects the appropriate lock:
`&mut self` acquires a write lock (serialized), `&self` acquires a read lock (concurrent).
Mixed signatures in the same impl block work transparently.

### State is struct fields

Plugin state is just normal struct fields — no wrapper types, no manual downcasting:

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

`&mut self` handlers run one at a time per actor, `&self` handlers run concurrently.
State is wrapped in `tokio::sync::RwLock` internally — no `Mutex` boilerplate.

### Custom initialization

`#[plugin]` doesn't inject `Default` — you add it explicitly, or supply `init`:

```rust
#[plugin(id = "my_plugin", name = "My Plugin", init = "MyPlugin::new()")]
struct MyPlugin {
    count: u64,
}

impl MyPlugin {
    fn new() -> Self { Self { count: 42 } }
}
```

### Match patterns

```rust
#[command("/ping")]                     // exact match (default)
#[command("/admin", kind = "prefix")]   // starts-with match
#[command(pattern = r"\d+", kind = "regex")]  // regex match
#[command(fallback)]                    // catch-all

#[message(keyword = "最低多少钱")]       // keyword match
```

### Event handlers

For business events like order placement, item purchase:

```rust
#[event("order_create")]
async fn on_order(&self, ctx: Context) -> Result<()> {
    tracing::info!("order: {:?}", ctx.payload()?);
    Ok(())
}
```

Event handlers have their own `Context` variant — `ctx.reply()`, `ctx.text()`,
`ctx.sender_id()` will return `Err` (they only make sense for message contexts).
Use `ctx.event_type()` and `ctx.payload()` instead.

### Context API

```
Message context methods (available in #[command] / #[message]):
  ctx.reply("hello").await?     — send a reply
  ctx.sender_id()?              — the sender's user ID
  ctx.cid()?                    — conversation/channel ID
  ctx.text()?                   — plain text content
  ctx.event()?                  — raw MessageEvent

System context methods (available in #[event]):
  ctx.event_type()?             — event type string
  ctx.payload()?                — event JSON payload

Common methods:
  ctx.adapter()                 — send arbitrary messages
  ctx.app_ctx()                 — dependency injection container
  ctx.telemetry()               — observability counters
```

Calling a message method in a system handler (or vice versa) returns
`AppError::Internal` — no panics.

## PluginBuilder (no macros)

For programmatic plugin construction or when proc macros aren't suitable:

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

Stateful variant with explicit initial state:

```rust
PluginBuilder::new("counter", "Counter")
    .state(0u64)
    .command("incr", "/incr", Arc::new(|cx: HandlerContext| {
        Box::pin(async move {
            let state = cx.plugin_state.clone().expect("stateful plugin");
            let lock = state.downcast::<parking_lot::RwLock<u64>>().expect("type");
            let mut val = lock.write();
            *val += 1;
            cx.event.reply(MessageSegment::text(format!("Count: {}", *val))).await;
            Ok(())
        })
    }))
    .build()
    .register();
```

### Capabilities and runtime config

```rust
PluginBuilder::new("my_plugin", "My Plugin")
    .capability(Capability::Network)
    .capability(Capability::SendMessage)
    .concurrency(16)
    .timeout(Duration::from_secs(10))
    .queue_strategy(QueueStrategy::DropOldest(100))
    .build();
```

| Capability | Meaning |
|---|---|
| `Network` | Outbound HTTP requests |
| `FileSystem` | Read local files |
| `FileSystemWrite` | Write local files |
| `SendMessage` | Send via adapter |
| `ReadAppContext` | Read Ctx container |

## Rule system

Rules are composable predicates over `MessageEvent`:

```rust
use fish_core::rule::*;

// Combinators
is_startswith("/admin").and(&is_keywords("delete"))
is_fullmatch(["/help", "/h", "帮助"])
is_regex(r"^\d{11}$").or(&is_fullmatch(["/phone"]))

// Custom
Rule::new(|event: &MessageEvent| event.has_image())
```

The `RouteHint` tells the Bot how to index the handler; the `Rule` is the actual
matcher inside the PluginActor. They should agree but serve different purposes:
- `RouteHint` is for routing (performance)
- `Rule` is for matching (semantics)

## System events

闲鱼 pushes business events through the WebSocket (not as chat messages).
The adapter automatically classifies `redReminder` payloads:

| redReminder | Mapped event | Meaning |
|---|---|---|
| `1` | `order_create` | Buyer placed order |
| `2` | `order_closed` | Transaction closed |
| `3` | `item_purchased` | Item sold |

These are delivered via `event_routes` in the Bot and received by
`#[event("order_create")]` handlers.

## Message protocol

Incoming messages follow a decrypt chain:

```
base64 → msgpack → JSON (recursive for nested fields)
```

1. Decode base64
2. Deserialize with `rmp-serde` into JSON Value
3. Fall back to direct JSON parse if msgpack fails
4. Recursively decrypt known nested fields

The filter layer drops:
- Typing indicators (input status)
- System pushes with `needPush=false`
- Empty or malformed payloads

## Queue strategy

When a plugin reaches its concurrency limit:

| Strategy | Behavior |
|---|---|
| `DropNewest` (default) | Drop the new event immediately |
| `DropOldest(n)` | Bounded queue; drop oldest, append newest |

This prevents slow plugins from building unbounded backlogs.

## Telemetry

18 atomic counters, logged automatically every 60 seconds:

| Layer | Counters |
|---|---|
| Routing | `messages_received`, `exact_route_hits`, `unmatched_messages`, `handler_dispatches` |
| Handler | `handler_started`, `handler_succeeded`, `handler_failed`, `handler_timed_out` |
| Queue | `drop_newest_drops`, `drop_oldest_enqueues`, `drop_oldest_oldest_discards`, `queued_handler_succeeded/failed/timed_out` |

Access from handlers: `ctx.telemetry().handler_started.fetch_add(1, ...)`.

## Dependency injection

```rust
// Inject at startup
let ctx = Arc::new(Ctx::new());
ctx.insert(my_db_pool);

// Retrieve in any handler
let pool = ctx.app_ctx().get::<PgPool>();
```

`Ctx` is a `TypeId`-keyed container with `parking_lot::RwLock` — no static lifetimes needed.

## Error handling

`AppError` is a `snafu`-based error enum with constructors for each category:

```rust
pub enum AppError {
    Http,       // HTTP request failures
    Ws,         // WebSocket errors
    Json,       // serde_json errors (auto-converted via From)
    Base64,     // base64 decode errors (auto-converted)
    Auth,       // authentication failures
    Protocol,   // protocol-level errors
    Internal,   // internal logic errors (context mismatch, invariants)
}
```

No unwraps in production code paths. `parking_lot` locks don't poison.
`serde_json` and `base64` errors auto-convert with `?`.

## Crate map

```
fish-core          Core data types (MessageEvent, SystemEvent, Rule, AppError, Ctx, Telemetry)
fish-adapter       Platform adapter (WebSocket, API, auth, protocol, MTOP signing)
fish-plugin        Plugin trait, MessageHandler, EventHandler, registry
fish-runtime       Actor runtime (PluginActor, dispatch, queue strategy)
fish-plugin-sdk    Unified SDK entry: re-exports, Context, Builder, prelude
fish-plugin-macros Proc macros: #[plugin] + #[plugin_handlers]
fish-bot           Binary entry point, Bot router, wiring
```

Dependency direction (top-to-bottom):

```
fish-bot → fish-runtime → fish-plugin-sdk → fish-plugin → fish-adapter → fish-core
fish-bot → fish-adapter → fish-core
fish-plugin-macros (independent proc-macro crate)
```

Every dependency is one-way. No cycles.

## License

MIT OR Apache-2.0
