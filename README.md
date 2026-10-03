# task26100571

ChalkSync：图层清空与限时撤销内核（纯 Python 标准库，无外部依赖）。

- `chalksync.py` — 库。`Replica.emit(action, now_ms, ...)` 生成可 JSON 序列化的事件，
  `Replica.receive(event)` 容忍乱序/重复/任意延迟，`Replica.visible(layer)` 返回当前可见笔画。
- `test_chalksync.py` — 自动化测试（unittest）。

运行测试：

```sh
python3 -m unittest -v
```
