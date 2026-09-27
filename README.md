# pubsub-message-middleware-go

本地（进程内）消息中间件：多个模块按主题（topic）发布订阅，
在订阅方变慢、掉线或消息版本对不上时，投递结果依然一致、可解释。

## 快速开始

```go
b := pubsub.NewBroker(pubsub.Config{
    MaxInFlight:        64,
    BufferSize:         64,
    BackpressurePolicy: pubsub.Block, // 或 pubsub.FailFast
})
defer b.Close()

sub, _ := b.Subscribe("worker-1", "orders")   // 具名订阅，断线可续传
seq, _ := b.Publish(ctx, "orders", pubsub.Message{ID: "order-1", Payload: data})

d, _ := sub.Next(ctx)   // 取出一条投递
sub.Ack(d)              // 确认后位置才推进
```

## 投递语义

- **至少一次（at-least-once）**：消息只有被 `Ack` 后才推进已确认位置（offset）。
  未确认的消息在订阅断线后不会丢，同名重连时重新投递。
- **有序追加**：每个主题是一条追加式日志，`Seq` 从 1 开始单调递增，
  同一订阅按序号顺序投递。
- **重连续传**：订阅以名字标识。`Close` 后同名再 `Subscribe`，
  从上次确认位置继续；断线前已投递未确认的消息会以
  `Redelivered=true`、`Attempt>1` 重投——**重复投递可以被明确识别**，
  消费方可据此做幂等处理。
- **确认幂等**：重复 `Ack` 或 `Ack` 已确认的消息不会重复记账；
  乱序 `Ack` 时位置只按连续序号推进。
- **同名唯一**：同一名字的订阅同时只允许一个活跃实例，
  防止两处消费导致记账混乱。

## 版本兼容规则

`Codec` 负责序列化与兼容校验，线上格式为 `{version, body}` 信封：

| 情况 | 结果 | 错误（可用 `errors.Is` 区分） |
| --- | --- | --- |
| 版本不在 `[MinVersion, MaxVersion]` | 拒绝 | `ErrVersionMismatch` |
| 出现订阅方不认识的字段 | 拒绝 | `ErrUnknownField`（含字段名） |
| 必填字段缺失或为 null | 拒绝 | `ErrMissingField`（含字段名） |
| 报文不是合法 JSON 信封 | 拒绝 | 解析错误 |

三种拒绝原因互不相同，不会静默跳过。互通规则：**同一主题下，
只要消息版本落在消费方声明的区间内且字段校验通过，即可互通；
否则以可区分的原因拒绝**。

```go
c := pubsub.NewCodec(1, 2) // 接受 v1~v2
data, _ := c.Encode(2, map[string]any{"id": "m-1", "amount": 42})
ver, body, err := c.Decode(data,
    []string{"id", "amount"},          // 必填字段
    []string{"id", "amount", "note"},  // 已认识字段
)
```

## 背压策略

每个活跃订阅的积压上限为 `MaxInFlight + BufferSize`
（积压 = 已追加到日志但未确认的消息数）。超限后：

- `Block`：发布方阻塞，直到订阅方确认释放出空间或 `ctx` 取消；
- `FailFast`：发布方立即收到 `ErrBackpressure`（含主题与订阅名），
  消息**不会部分写入**，不会出现"写了一半"的状态。

两种策略都是确定的，不会无声丢数据。**已断线的订阅不施加背压**，
其应得的消息保留在内存日志中，重连后续传
（进程内实现，日志不持久化，进程退出即丢失）。

## 并发与一致性

- Broker 内部所有共享状态（日志、订阅位置、在途集合）由单一把互斥锁保护，
  位置推进与背压判定在同一临界区内完成，不会竞态或重复记账。
- 每个订阅有独立的投递泵（pump）goroutine，按 `MaxInFlight` 限制在途数量。
- 并发正确性由 `go test -race` 下的并发发布订阅、并发重连测试覆盖。

## 配置项

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `MaxInFlight` | 64 | 每个订阅允许的最大未确认（在途）消息数 |
| `BufferSize` | 64 | 每个订阅投递通道的缓冲长度 |
| `BackpressurePolicy` | `Block` | 积压超限策略：`Block` 或 `FailFast` |

## 测试

```sh
go test ./...                 # 全部单测
CGO_ENABLED=1 go test -race ./...   # 含竞态检测
```

覆盖场景：重连续传与重投识别、确认幂等与乱序确认、
版本不兼容/未知字段/缺失字段的区分拒绝、FailFast 与 Block 背压、
并发发布订阅、并发断开重连。单测日志会打印消息 ID 与确认位置，
便于定位问题。
