# ChalkSync

图层清空与限时撤销内核，纯 Python 标准库，无外部依赖。最多 16 个副本、
两个固定图层（0 和 1），副本之间只交换 `emit` 生成的 JSON 事件。

## API

```python
from chalksync import Replica

r = Replica("A")                                   # author id 唯一
e1 = r.emit({"op": "add", "layer": 0,
             "content": {"text": "hi"}}, now_ms=0) # -> 可 JSON 序列化的事件
e2 = r.emit({"op": "clear", "layer": 0}, now_ms=100)
e3 = r.emit({"op": "undo", "target": e2},
            now_ms=100 + 9999)                     # >= 10000ms 返回 None
other.receive(json.loads(json.dumps(e1)))          # 允许乱序/重复/任意延迟
r.visible(0)  # -> [(stroke_id, content), ...]，顺序确定，各副本一致
```

- clear 的覆盖集合 = 生成时已生效（因果过去）的同图层 add，用版本向量
  表达，事件体不含逐笔画列表，clear/undo 编码远小于 4 KiB。
- undo 只取消目标 clear 自身的隐藏作用；签发窗口为 clear 生成后
  `< 10000` ms（恰好 10000 拒绝且不改变状态），合法 undo 晚到仍有效。
- 因果前驱未齐备的事件被缓冲，不影响视图，也不计入新事件的观察范围。

## 运行测试

```sh
python3 -m unittest test_chalksync -v
```
