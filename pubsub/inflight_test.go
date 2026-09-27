package pubsub

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestMaxInFlightConcurrency: with MaxInFlight>1 several messages are handled
// concurrently (the slow head does not block dispatch of later messages),
// while commit still advances contiguously only after the head is acked.
func TestMaxInFlightConcurrency(t *testing.T) {
	reg := testRegistry(t)
	b := NewBroker(reg, DefaultBrokerConfig())
	defer b.Close()

	var active int32
	var maxActive int32
	gate := make(chan struct{})
	allIn := make(chan struct{})
	var entered int32

	sub, err := b.Subscribe("orders", 1, HandlerFunc(func(ctx context.Context, d Delivery) error {
		cur := atomic.AddInt32(&active, 1)
		for {
			m := atomic.LoadInt32(&maxActive)
			if cur <= m || atomic.CompareAndSwapInt32(&maxActive, m, cur) {
				break
			}
		}
		if atomic.AddInt32(&entered, 1) == 4 {
			close(allIn)
		}
		<-gate // hold all 4 concurrently
		atomic.AddInt32(&active, -1)
		t.Logf("handled id=%s offset=%d", d.Message.ID, d.Message.Offset)
		return nil
	}), WithBackpressure(BackpressureConfig{PendingLimit: 64, MaxInFlight: 4, Policy: PolicyBlock}))
	if err != nil {
		t.Fatal(err)
	}

	publishN(t, b, "orders", 4)
	select {
	case <-allIn:
	case <-time.After(2 * time.Second):
		t.Fatalf("only %d/4 handlers entered concurrently (max=%d)", atomic.LoadInt32(&entered), atomic.LoadInt32(&maxActive))
	}
	if got := atomic.LoadInt32(&maxActive); got != 4 {
		t.Fatalf("max concurrent handlers = %d, want 4 (MaxInFlight not honoured)", got)
	}
	// Head not acked yet -> commit must still be -1 despite concurrent work.
	if c := sub.CommitPosition(); c != -1 {
		t.Fatalf("commit=%d, must stay -1 until contiguous head acks", c)
	}
	close(gate)
	waitCommit(t, sub, 3)
}
