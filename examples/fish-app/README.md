# Fish App Example

真实宿主骨架，直接使用 `fish-rt-adapter` 提供的 `FishWebSocketAdapter`。

用途：

- 展示 runtime 的标准接线方式
- 作为你后续业务接入的起点

运行前建议准备本地认证信息：

- `FISH_AUTH_JSON`
- 或 `FISH_DATA_DIR/fish_auth.json`

运行：

```bash
cargo run -p fish-example-fish-app
```

这个示例会真的连接闲鱼 websocket，并持续运行。
