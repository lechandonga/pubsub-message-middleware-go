package pubsub

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func publishN(t *testing.T, b *Broker, topic string, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("%s/msg-%d", topic, i)
		seq, err := b.Publish(context.Background(), topic, Message{ID: id})
		if err != nil {
			t.Fatalf("Publish %s: %v", id, err)
		}
		t.Logf("published id=%s seq=%d", id, seq)
	}
}

func TestBasicPubSub(t *testing.T) {
	b := NewBroker(Config{})
	defer b.Close()
	sub, err := b.Subscribe("s1", "orders")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	publishN(t, b, "orders", 3)
	for i := 1; i <= 3; i++ {
		d, err := sub.Next(context.Background())
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if d.Redelivered || d.Attempt != 1 {
			t.Fatalf("first delivery should not be redelivered: %+v", d)
		}
		if err := sub.Ack(d); err != nil {
			t.Fatalf("Ack: %v", err)
		}
		t.Logf("acked id=%s seq=%d offset=%d", d.ID, d.Seq, sub.Offset())
	}
	if got := sub.Offset(); got != 3 {
		t.Fatalf("offset = %d, want 3", got)
	}
}

// 重连续传：消费一半后断线，同名重连后从上次确认位置继续，
// 已投递未确认的消息以 Redelivered=true 重投。
func TestReconnectResume(t *testing.T) {
	b := NewBroker(Config{})
	defer b.Close()
	ctx := context.Background()

	sub, err := b.Subscribe("worker-1", "jobs")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	publishN(t, b, "jobs", 4)

	// 收到 1、2，只确认 1，然后模拟掉线
	d1, _ := sub.Next(ctx)
	d2, _ := sub.Next(ctx)
	if err := sub.Ack(d1); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	t.Logf("before disconnect: acked id=%s seq=%d offset=%d (id=%s seq=%d delivered but unacked)",
		d1.ID, d1.Seq, sub.Offset(), d2.ID, d2.Seq)
	sub.Close()

	// 同名重连
	sub2, err := b.Subscribe("worker-1", "jobs")
	if err != nil {
		t.Fatalf("re-Subscribe: %v", err)
	}
	if got := sub2.Offset(); got != 1 {
		t.Fatalf("resumed offset = %d, want 1", got)
	}
	// 掉线前 seq=2,3,4 都已投递（在投递队列中）但未确认，
	// 重连后应全部以 Redelivered=true、Attempt=2 重投，且不丢不重
	for want := uint64(2); want <= 4; want++ {
		d, err := sub2.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if d.Seq != want || !d.Redelivered || d.Attempt != 2 {
			t.Fatalf("want redelivered seq=%d attempt=2, got %+v", want, d)
		}
		t.Logf("after reconnect: redelivered id=%s seq=%d attempt=%d offset=%d",
			d.ID, d.Seq, d.Attempt, sub2.Offset())
		sub2.Ack(d)
		t.Logf("acked id=%s seq=%d offset=%d", d.ID, d.Seq, sub2.Offset())
	}
	// 没有更多消息：Next 应阻塞，用超时 ctx 验证不重复投递
	shortCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := sub2.Next(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Next after drain err = %v, want DeadlineExceeded (no duplicates)", err)
	}
	if got := sub2.Offset(); got != 4 {
		t.Fatalf("final offset = %d, want 4", got)
	}
}

// Ack 幂等：重复确认不重复记账。
func TestAckIdempotent(t *testing.T) {
	b := NewBroker(Config{})
	defer b.Close()
	sub, _ := b.Subscribe("s1", "t")
	publishN(t, b, "t", 2)
	ctx := context.Background()
	d1, _ := sub.Next(ctx)
	d2, _ := sub.Next(ctx)
	sub.Ack(d1)
	sub.Ack(d1) // 重复确认
	sub.Ack(d2)
	sub.Ack(d1) // 确认一个更早的
	if got := sub.Offset(); got != 2 {
		t.Fatalf("offset = %d, want 2 (duplicate acks must not double-count)", got)
	}
	t.Logf("duplicate acks ignored, offset=%d", sub.Offset())
}

// 乱序确认：位置只连续推进。
func TestOutOfOrderAck(t *testing.T) {
	b := NewBroker(Config{})
	defer b.Close()
	sub, _ := b.Subscribe("s1", "t")
	publishN(t, b, "t", 3)
	ctx := context.Background()
	d1, _ := sub.Next(ctx)
	d2, _ := sub.Next(ctx)
	d3, _ := sub.Next(ctx)
	sub.Ack(d2)
	if got := sub.Offset(); got != 0 {
		t.Fatalf("offset = %d, want 0 before contiguous ack", got)
	}
	sub.Ack(d1)
	sub.Ack(d3)
	if got := sub.Offset(); got != 3 {
		t.Fatalf("offset = %d, want 3", got)
	}
	t.Logf("out-of-order acks settled, offset=%d", sub.Offset())
}

// FailFast 背压：订阅方不消费时，发布按确定策略被拒绝，而不是静默丢。
func TestBackpressureFailFast(t *testing.T) {
	b := NewBroker(Config{MaxInFlight: 2, BufferSize: 1, BackpressurePolicy: FailFast})
	defer b.Close()
	if _, err := b.Subscribe("slow", "t"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// 上限 = MaxInFlight+BufferSize = 3，第 4 条应被拒绝
	for i := 1; i <= 3; i++ {
		if _, err := b.Publish(context.Background(), "t", Message{}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	_, err := b.Publish(context.Background(), "t", Message{})
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("Publish err = %v, want ErrBackpressure", err)
	}
	t.Logf("overload rejected deterministically: %v", err)
}

// Block 背压：发布方阻塞，订阅方确认后被放行。
func TestBackpressureBlock(t *testing.T) {
	b := NewBroker(Config{MaxInFlight: 1, BufferSize: 1, BackpressurePolicy: Block})
	defer b.Close()
	sub, _ := b.Subscribe("slow", "t")
	ctx := context.Background()
	if _, err := b.Publish(ctx, "t", Message{ID: "t/msg-1"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if _, err := b.Publish(ctx, "t", Message{ID: "t/msg-2"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// 第 3 条会阻塞，直到订阅方确认
	done := make(chan error, 1)
	go func() {
		_, err := b.Publish(ctx, "t", Message{ID: "t/msg-3"})
		done <- err
	}()
	select {
	case <-done:
		t.Fatal("Publish should block while subscriber is overloaded")
	case <-time.After(100 * time.Millisecond):
	}
	d, err := sub.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	sub.Ack(d)
	t.Logf("acked id=%s seq=%d offset=%d, blocked publisher should proceed", d.ID, d.Seq, sub.Offset())
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("blocked Publish: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Publish did not proceed after ack")
	}
}

// 同名订阅仍活跃时不允许重复订阅。
func TestDuplicateActiveSubscription(t *testing.T) {
	b := NewBroker(Config{})
	defer b.Close()
	if _, err := b.Subscribe("s1", "t"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := b.Subscribe("s1", "t"); err == nil {
		t.Fatal("duplicate active subscription should be rejected")
	}
}
