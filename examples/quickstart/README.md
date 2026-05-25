# Quickstart Example

离线示例，展示一个宿主如何通过 `RuntimeHost` 组装：

- `PluginActor`
- `Bot`
- `BaseAdapter` 事件下推

运行：

```bash
cargo run -p fish-example-quickstart
```

预期输出会包含两次 `send -> ...`，分别来自：

- `/ping` 命中精确命令后的 `pong`
- `hello fish runtime` 命中关键词后的回包
