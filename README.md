# pubsub-message-middleware-go

一个纯 Go、进程内（in-process）的本地消息中间件：多个模块之间按**主题**发布 /
订阅，并在订阅方**变慢、短暂掉线、或消息结构版本不匹配**时，保证投递一致、
可复现、可解释，且不静默丢消息。

```go
import "github.com/lechandonga/pubsub-message-middleware-go/pubsub"
```

## 特性

- **按主题发布订阅**，每个主题是稠密、单调的 offset 日志（从 0 开始）。
- **至少一次投递 + 顺序提交**：消息重投到确认为止；commit 只沿连续头部推进，
  任何消息都不会被跳过。
- **可识别的重复投递**：`Message.ID` 在重投间保持不变，`Message.Attempt`
  每次重投严格递增；配套 `DedupTracker` 做幂等去重。
- **掉线重连续传**：`Detach/Attach` 或同名（`WithSubscriptionName`）重订阅都从
  上次确认位置 `commit+1` 继续，掉线期间消息不丢。
- **结构版本兼容规则明确**：严格 JSON codec + 显式版本迁移器；缺字段、未知字段、
  版本缺失/未知、版本不兼容分别返回不同的 `RejectReason`；无法解码的消息
  “停放而非跳过”，可隔离（Ack）或重试（Nack）。
- **确定的背压策略**：`PolicyBlock`（阻塞、不丢数据，默认）或
  `PolicyReject`（显式返回 `ErrBackpressure`，可重试），按订阅配置
  `PendingLimit / MaxInFlight`。
- **并发安全**：明确锁顺序，准入 FIFO 公平有界；`go test -race` 覆盖。
- **可观测日志**：打印消息 ID、offset、attempt、commit、拒绝原因。

## 快速开始

```go
package main

import (
    "context"
    "fmt"

    "github.com/lechandonga/pubsub-message-middleware-go/pubsub"
)

func main() {
    reg := pubsub.NewRegistry()
    // v1 schema：id/amount 必填。
    reg.Register("orders", pubsub.NewJSONCodec(1, []pubsub.JSONField{
        {Name: "id", Required: true},
        {Name: "amount", Required: true},
    }))

    b := pubsub.NewBroker(reg, pubsub.BrokerConfig{
        AutoCreateTopics: true,
        Logger:           pubsub.NewStdLogger(nil), // 打印 id/offset/commit
    })
    defer b.Close()

    _, _ = b.Subscribe("orders", 1,
        pubsub.HandlerFunc(func(ctx context.Context, d pubsub.Delivery) error {
            fmt.Printf("got id=%s offset=%d attempt=%d payload=%v\n",
                d.Message.ID, d.Message.Offset, d.Message.Attempt, d.Payload)
            return nil // nil 自动 Ack；返回 error 会以 Attempt+1 重投
        }),
        pubsub.WithSubscriptionName("svc-a"),       // 具名：重连后续传
        pubsub.WithBackpressure(pubsub.BackpressureConfig{
            PendingLimit: 256, MaxInFlight: 1, Policy: pubsub.PolicyBlock,
        }),
    )

    _, _ = b.Publish("orders", 1,
        map[string]any{"id": "o-1", "amount": 100},
        pubsub.WithMessageID("o-1"))
}
```

## 版本兼容

```go
// v2 在 v1 基础上增加可选字段 currency。
reg.Register("orders", pubsub.NewJSONCodec(2, []pubsub.JSONField{
    {Name: "id", Required: true},
    {Name: "amount", Required: true},
    {Name: "currency", Required: false},
}))
// 旧消息给新订阅方：显式向上迁移（补默认值）。
reg.RegisterMigrator("orders", 1, 2, func(v any) (any, error) {
    m := v.(map[string]any)
    if _, ok := m["currency"]; !ok {
        m["currency"] = "USD"
    }
    return m, nil
})
```

没有显式迁移器的跨版本（尤其“新消息→旧订阅方”）会得到
`RejectIncompatibleVersion`，配合 `WithRejectHandler` 隔离或重试，绝不静默跳过。

## 运行测试

```bash
go test ./...            # 无需 CGO
go test -race ./...      # 竞态检测（需要 CGO/gcc）
go test -race -v ./...   # 同时打印每条消息的 id/offset/attempt/commit
```

测试覆盖：重连续传与重复识别、版本/字段不兼容的区分拒绝、并发发布订阅与
背压（阻塞 / 显式拒绝）、并发确认记账。

## 文档

完整语义、兼容规则、背压策略与全部配置项见 [`docs/design.md`](docs/design.md)。
