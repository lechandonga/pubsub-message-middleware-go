# 本地消息中间件设计说明（pubsub）

进程内（in-process）、按主题（topic）的发布/订阅中间件。核心目标是：**当订阅方
变慢、短暂掉线（detach）、或消息结构版本对不上时，投递结果仍然一致、可复现、
可解释，绝不静默丢消息。**

- 包路径：`github.com/lechandonga/pubsub-message-middleware-go/pubsub`
- 纯 Go 标准库实现，无外部依赖。
- 关键类型：`Broker`、`Subscription`、`Delivery`、`Registry`、`JSONCodec`、
  `BackpressureConfig`、`DedupTracker`、`RejectError`。

---

## 1. 数据模型与可复现标识

每个主题是一条**追加式、稠密、单调的偏移量日志（offset log）**：

- 第一条消息偏移量为 `0`，之后依次 `1,2,3,...`，中间不空洞。
- 每条消息 `Message` 同时携带：
  - `Offset`：该主题内的确定位置；
  - `ID`（`MessageID`）：生产者指定的消息标识（`WithMessageID`），
    缺省时由 broker 生成。**同一条消息的每次重投，ID 不变**；
  - `Attempt`：本次订阅方的投递序号。首次为 `1`，每次重投严格 `+1`。

因此订阅方可以用两种互补方式识别“重复投递”：

1. `Message.Attempt > 1`：这是同一位置的再投递；
2. `Message.ID` 配合 `DedupTracker`：在一个滑动窗口内记录已处理 ID，
   `Mark(id)` 返回 `true` 即重复，避免重复记账造成状态错乱。

> 投递语义是 **at-least-once（至少一次）+ 按偏移顺序提交**。重复必然可识别，
> 业务侧用 ID 做幂等即可得到“恰好生效一次”的效果。

---

## 2. 投递语义（明确且可复现）

### 2.1 派发与确认（Ack/Nack）

- 每个订阅有一个**已确认位置 `commit`**（最高的、连续确认的偏移量）。
- dispatcher 严格按偏移顺序派发；默认 `MaxInFlight = 1`（一次只有一条在途），
  保证严格有序、可恢复。
- 处理函数返回 `nil` 即自动 Ack；返回 error 即自动 Nack。也可以通过
  `Delivery.Ack()` / `Delivery.Nack()` 或 `Subscription.Ack(off)` 显式控制。
- **commit 只在“连续头部”推进**：偏移 5 先确认而 4 未确认时，commit 不动；
  等 4 确认后一次性推进到 5。因此任何空洞都不可能被“跳过”。
- Nack 不移动 commit，该消息以 `Attempt+1` 重新投递。
- 对同一偏移重复 Ack：返回 `ErrAlreadyAcked`；
  Ack 一个既不在途也未因解码被“停放”的偏移：返回 `ErrBadAck`。
  这些都是确定性的、可解释的结果，不会重复记账。

### 2.2 慢订阅方 / 短暂掉线：重连续传，不丢消息

- `Subscription.Detach()`：订阅方临时离线。在途派发槽被回收，commit 保留。
- `Subscription.Attach(ctx)`：重新上线，**严格从 `commit+1` 继续**，
  所有未提交消息重新投递，`Attempt > 1` 清晰标识其为重投。
- `WithSubscriptionName("name")`：具名订阅在 broker 内存放游标。即使关闭旧
  句柄、稍后用**同一个名字**重新订阅，也会从上次 commit 之后继续，
  不会重放已确认的消息。
- 掉线期间发布的消息只增不减地保存在主题日志中，重连后补齐。
- 日志裁剪（`BrokerConfig.Retention`）只会删除**所有订阅都已确认过的前缀**，
  因此续传永远不会落到一条已经不存在的消息上。

### 2.3 订阅方是独立“视图”

不同订阅（不同名字）各自维护独立 commit，互不影响：一条消息会被每个活跃
订阅各投递一次，各自独立 Ack。

---

## 3. 序列化与结构版本兼容（规则明确，拒绝可区分）

`Registry` 为每个主题注册若干版本的 `Codec`（自带一个严格 JSON 实现
`JSONCodec`），以及**显式**注册的版本迁移器 `Migrator`。

### 3.1 解码判定顺序（确定性）

1. 消息携带版本 `v == 0` → 拒绝：`schema_version_missing`（绝不隐式猜版本）。
2. 没有为 `v` 注册 codec → 拒绝：`schema_version_unknown`。
3. `v == want`：直接用该版本 codec 解码。
4. `v != want`：必须存在**显式注册**的 `v -> want` 迁移器；解码后套用。
   - 没有迁移器（典型：新消息发给只懂旧版本的订阅方）→
     **拒绝：`incompatible_version`**，不会被悄悄跳过或强行降级。
   - 旧消息 → 新订阅方：需要显式的“向上迁移”（可为新增字段填默认值）。
   - 新消息 → 旧订阅方：默认拒绝；只有显式注册了“向下迁移”才兼容。

### 3.2 严格字段规则（`JSONCodec`）

- 必填字段缺失或为 null → `missing_required_field`（错误里带字段名）；
- 出现目标 schema 未声明的字段 → `unknown_field`（带字段名）；
- 不是合法 JSON 对象 / 多段 JSON → `malformed_payload`。

所有拒绝都返回 `*RejectError`，稳定字段：`Reason / Topic / ID /
FromVersion / WantVersion / Field / Detail`，用 `AsRejectError(err)` 分类。
字段缺失、未知字段、版本不匹配是**三种不同的拒绝原因**，调用方可以明确区分。

### 3.3 解码失败时“停放而不是跳过”

当某条消息无法按订阅方版本解码时：

- 若配置了 `WithRejectHandler`：调用它，应用可选择
  - `d.Ack()`：例如转入隔离区（quarantine）并人工处理，然后放行；
  - `d.Nack()`：稍后重试。
- 若未配置：订阅**确定性地停在该偏移**，后续消息不会越过它投递。
  可用 `Subscription.BlockedOn()` 查看被停放的偏移与拒绝原因。

这样“版本对不上”永远不会表现为静默丢消息。

---

## 4. 背压与并发控制（确定策略，不无声丢数据）

### 4.1 配置项（`BackpressureConfig`，每订阅可覆盖）

| 字段 | 默认 | 含义 |
| --- | --- | --- |
| `PendingLimit` | `1024` | 允许某个订阅积压的**未确认消息总数**（含在途的 1 条）。对该订阅，准备发布第 N 条后未确认数 = `logEnd - commit`；超过即触发策略。 |
| `MaxInFlight` | `1` | 单个订阅同时在途（已派发未确认）的消息数。`1` 保证严格有序、可恢复；调大允许并发处理但放弃逐条顺序（commit 仍按连续头部推进）。 |
| `Policy` | `PolicyBlock` | 过载策略，见下。 |

### 4.2 两种确定的过载策略

- **`PolicyBlock`（默认，推荐）**：当最慢的活跃订阅会超过 `PendingLimit` 时，
  `Publish` 阻塞等待，直到该订阅 Ack 腾出空间，再重新判定。等待期间释放内部
  锁，确认操作始终能前进。**不丢任何数据**，背压沿发布方向确定性传导。
- **`PolicyReject`**：同样情况下 `Publish` **在消息进入日志之前**返回
  `ErrBackpressure`（可 `errors.Is` 判定，错误中含 topic/lag/limit）。
  生产者得到显式、可重试的失败，而不是无声丢弃；已在日志中的消息不受影响。

多个订阅使用不同策略时：**Reject 优先于 Block**（混合策略下确定性地快速失败）。

### 4.3 公平性与有界性

- 每个主题一把“准入锁”（admission mutex）+ `sync.Cond`：同一时刻只有一个
  发布者做准入判定；需要等待的发布者排队，被 Ack 广播唤醒后**重新判定**。
  一个尚未打满的后来发布者无法“插队”抢走正在等待者的名额，因此既公平又有界。
- 每次连续 Ack 都会广播唤醒；Detach / Close 同样广播，避免发布者永久等待。

### 4.4 掉线订阅不施压

Detach（离线）的订阅**不参与背压**：消息继续进日志累积，发布方不被它阻塞；
重新 Attach 后，新的发布会依据当前积压重新施加背压。

### 4.5 并发安全与锁约定

- 所有可变状态都在锁内更新；订阅 commit/在途/尝试计数由 `subState.mu` 保护，
  主题日志由 `topicLog.mu` 保护。
- 严格锁顺序：`broker.mu → topic.mu → sub.mu`。dispatcher 读取日志尾或取消息时
  只持有 `topic.mu`，绝不持 `sub.mu` 反向获取，杜绝 ABBA 死锁。
- 测试在 `-race` 下覆盖多发布者/多订阅者并发与重复 Ack/Nack 记账。

---

## 5. 可观测性：日志带消息标识与确认位置

`BrokerConfig.Logger` 可注入 `Logger`，内置 `NewStdLogger(...)` 输出每事件一行：

- `delivered  topic=... id=... offset=... attempt=...`
- `redeliver  topic=... id=... offset=... attempt=...`（重投，attempt>1）
- `acked      topic=... sub=... offset=... commit=...`（含推进后的确认位置）
- `rejected   topic=... id=... offset=... reason=...`（含可分类的拒绝原因）

单测也会打印 `id / offset / attempt / commit`，便于定位“从哪里续传、
哪条重复、停在哪个版本不兼容的消息上”。

---

## 6. 配置项汇总

### `BrokerConfig`

| 字段 | 默认（`DefaultBrokerConfig`） | 说明 |
| --- | --- | --- |
| `AutoCreateTopics` | `true` | 发布/订阅时自动创建主题；为 `false` 时对未知主题返回 `ErrUnknownTopic`。 |
| `Retention` | `0`（不限） | 每主题保留的消息条数上限；只会裁掉所有订阅都已确认的前缀。 |
| `Backpressure` | `DefaultBackpressureConfig()` | 所有订阅的默认背压配置。 |
| `Logger` | `nil`（不输出） | 诊断日志钩子；用 `NewStdLogger` 得到标准行式日志。 |

### 订阅选项

- `WithSubscriptionName(name)`：具名（可重连续传）订阅。
- `WithStartOffset(off)` / `WithStartLatest()`：**新**订阅的起始位置
  （对已有游标的具名重订阅无效）。
- `WithBackpressure(bp)`：覆盖该订阅的背压配置。
- `WithRejectHandler(rh)`：处理无法解码的消息（隔离 Ack 或重试 Nack）。

### 发布选项

- `WithMessageID(id)`：显式消息 ID（生产者重试时应复用同一 ID 以便去重）。
- `WithHeaders(map)`：附加消息头。

---

## 7. 最小用法

```go
reg := pubsub.NewRegistry()
reg.Register("orders", pubsub.NewJSONCodec(1, []pubsub.JSONField{
    {Name: "id", Required: true},
    {Name: "amount", Required: true},
}))

b := pubsub.NewBroker(reg, pubsub.DefaultBrokerConfig())
defer b.Close()

sub, _ := b.Subscribe("orders", 1,
    pubsub.HandlerFunc(func(ctx context.Context, d pubsub.Delivery) error {
        // d.Message.ID / .Offset / .Attempt 可见；Attempt>1 即重投
        return process(d.Payload) // 返回 nil 自动 Ack；返回 error 自动重投
    }),
    pubsub.WithSubscriptionName("svc-a"))

_, _ = b.Publish("orders", 1, map[string]any{"id": "o-1", "amount": 100})
```

---

## 8. 测试覆盖

`go test -race ./...`（需 CGO）覆盖：

- **重连续传**：`TestResumeAfterDetach`（Detach/Attach、具名重订阅从 commit+1
  继续、顺序不乱、旧消息不重放）；`TestRedeliveryAndDedup`
  （失败两次后 attempt=1,2,3、`DedupTracker` 识别重复、只提交一次）。
- **版本不兼容**：`TestJSONCodecStrict`（缺字段/未知字段/格式错各自原因）、
  `TestRegistryVersionRules`（同版本直解、显式向上/向下迁移、缺失/未知版本、
  隐式版本号 0）、`TestIncompatibleMessageParksThenResolves`
  （无法解码时停放，显式 Ack 隔离后继续，后续消息不丢）。
- **并发发布订阅 / 背压**：`TestConcurrentPublishSubscribe`（4 发布者 ×25 条 ×
  3 订阅，每偏移仅一条消息、每 ID 恰好一次、最终 commit 正确）、
  `TestBackpressureBlock`（超限确定性阻塞、开门后按序补齐不丢）、
  `TestBackpressureReject`（超限显式 `ErrBackpressure`、日志不被污染）、
  `TestConcurrentAckAccounting`（并发重复/越界 Ack 不重复记账、不 panic）。
