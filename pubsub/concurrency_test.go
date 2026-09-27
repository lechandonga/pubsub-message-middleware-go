package pubsub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentPublishSubscribe: N publishers each publish M messages to one
// topic; several subscribers (named) must each receive every message exactly
// once per committed offset, and final commit must be N*M-1 for every
// subscriber. Run under -race to verify commit/inflight accounting.
func TestConcurrentPublishSubscribe(t *testing.T) {
	reg := testRegistry(t)
	b := NewBroker(reg, BrokerConfig{AutoCreateTopics: true, Logger: NewStdLogger(nil)})
	defer b.Close()

	const publishers = 4
	const perPublisher = 25
	const subscribers = 3

	var total int64
	var wgPub sync.WaitGroup
	type subRec struct {
		seen    sync.Map // MessageID -> count
		offSeen sync.Map // Offset -> MessageID
		done    chan struct{}
		sub     *Subscription
	}
	recs := make([]*subRec, subscribers)
	for i := 0; i < subscribers; i++ {
		r := &subRec{done: make(chan struct{})}
		recs[i] = r
		name := fmt.Sprintf("worker-%d", i)
		var got int64
		s, err := b.Subscribe("orders", 1, HandlerFunc(func(ctx context.Context, d Delivery) error {
			if v, loaded := r.seen.LoadOrStore(d.Message.ID, 1); loaded {
				n := v.(int)
				r.seen.Store(d.Message.ID, n+1)
			}
			if prev, loaded := r.offSeen.LoadOrStore(d.Message.Offset, d.Message.ID); loaded && prev != d.Message.ID {
				t.Errorf("offset %d delivered two different IDs: %s vs %s",
					d.Message.Offset, prev, d.Message.ID)
			}
			t.Logf("sub=%s id=%s offset=%d attempt=%d", name, d.Message.ID, d.Message.Offset, d.Message.Attempt)
			if atomic.AddInt64(&got, 1) == publishers*perPublisher {
				select {
				case <-r.done:
				default:
					close(r.done)
				}
			}
			return nil
		}), WithSubscriptionName(name))
		if err != nil {
			t.Fatal(err)
		}
		r.sub = s
	}

	for p := 0; p < publishers; p++ {
		wgPub.Add(1)
		go func(p int) {
			defer wgPub.Done()
			for j := 0; j < perPublisher; j++ {
				id := MessageID(fmt.Sprintf("p%d-%d", p, j))
				m, err := b.Publish("orders", 1, map[string]any{"id": id, "amount": j}, WithMessageID(id))
				if err != nil {
					t.Errorf("publish %s: %v", id, err)
					return
				}
				atomic.AddInt64(&total, 1)
				_ = m
			}
		}(p)
	}
	wgPub.Wait()
	t.Logf("published total=%d", atomic.LoadInt64(&total))

	for i, r := range recs {
		select {
		case <-r.done:
		case <-time.After(5 * time.Second):
			t.Fatalf("subscriber %d timed out, commit=%d end-check", i, r.sub.CommitPosition())
		}
		waitCommit(t, r.sub, Offset(publishers*perPublisher-1))
		// Every ID exactly once.
		count := 0
		r.seen.Range(func(_, v any) bool {
			if v.(int) != 1 {
				t.Errorf("subscriber %d saw a message %v times (want 1)", i, v)
			}
			count++
			return true
		})
		if count != publishers*perPublisher {
			t.Errorf("subscriber %d saw %d unique messages, want %d", i, count, publishers*perPublisher)
		}
	}
}

// TestBackpressureBlock: once a subscriber exceeds PendingLimit, further
// publishes block deterministically (PolicyBlock) instead of dropping data,
// and all messages drain in order after the slow handler acks.
//
// The test makes the condition deterministic by first pinning commit=-1
// (handler parked on the head) before measuring admission.
func TestBackpressureBlock(t *testing.T) {
	reg := testRegistry(t)
	b := NewBroker(reg, DefaultBrokerConfig())
	defer b.Close()

	// pending = end - commit counts the in-flight head too. With commit=-1:
	// head pending=1, then 2,3,4; we want head + 2 extras admitted (pending up
	// to 3) and the 4th publish blocked, so limit = 3 blocks at pending 4.
	const limit = 3
	const total = 5
	gate := make(chan struct{})
	headParked := make(chan struct{}, 1)
	var parkOnce sync.Once
	sub, err := b.Subscribe("orders", 1, HandlerFunc(func(ctx context.Context, d Delivery) error {
		if d.Message.Offset == 0 {
			parkOnce.Do(func() { close(headParked) })
		}
		<-gate // never ack until the test opens the gate
		return nil
	}), WithBackpressure(BackpressureConfig{PendingLimit: limit, MaxInFlight: 1, Policy: PolicyBlock}),
		WithSubscriptionName("slow"))
	if err != nil {
		t.Fatal(err)
	}

	// 1) Publish the head and wait until its handler is parked: commit=-1.
	headErr := make(chan error, 1)
	go func() {
		_, e := b.Publish("orders", 1, map[string]any{"id": "b-0", "amount": 0}, WithMessageID("b-0"))
		headErr <- e
	}()
	select {
	case <-headParked:
		fmt.Println("DBG head parked")
	case <-time.After(2 * time.Second):
		t.Fatal("head handler never parked")
	}

	// 2) Two more messages are admitted, bringing pending to limit.
	for i := 1; i < limit; i++ {
		id := MessageID(fmt.Sprintf("b-%d", i))
		done := make(chan error, 1)
		go func() {
			_, e := b.Publish("orders", 1, map[string]any{"id": id, "amount": i}, WithMessageID(id))
			done <- e
		}()
		select {
		case e := <-done:
			if e != nil {
				t.Fatalf("publish %d admitted but failed: %v", i, e)
			}
		case <-time.After(2 * time.Second):
			fmt.Println("DBG stuck admitting", i)
			t.Fatalf("publish %d should be admitted within the limit", i)
		}
		fmt.Println("DBG admitted", i)
	}

	// 3) One more publish must block, not drop/reject.
	blockDone := make(chan error, 1)
	go func() {
		_, e := b.Publish("orders", 1, map[string]any{"id": "b-3", "amount": 3}, WithMessageID("b-3"))
		blockDone <- e
	}()
	select {
	case e := <-blockDone:
		t.Fatalf("over-limit publish returned (err=%v) instead of blocking", e)
	case <-time.After(100 * time.Millisecond):
		fmt.Println("DBG blocked as expected")
	}

	// 4) Queue the final message behind the blocked one.
	lastDone := make(chan error, 1)
	go func() {
		_, e := b.Publish("orders", 1, map[string]any{"id": "b-4", "amount": 4}, WithMessageID("b-4"))
		lastDone <- e
	}()

	// 5) Unblock: everything completes in order, nothing lost, commit advances.
	close(gate)
	for name, ch := range map[string]chan error{"head": headErr, "blocked": blockDone, "last": lastDone} {
		select {
		case e := <-ch:
			if e != nil {
				t.Fatalf("%s publish failed after unblock: %v", name, e)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("%s publish did not complete after unblock", name)
		}
	}
	waitCommit(t, sub, total-1)
}

// TestBackpressureReject: PolicyReject surfaces ErrBackpressure explicitly
// instead of silently dropping; messages already in the log are not lost.
func TestBackpressureReject(t *testing.T) {
	reg := testRegistry(t)
	b := NewBroker(reg, DefaultBrokerConfig())
	defer b.Close()

	gate := make(chan struct{})
	sub, err := b.Subscribe("orders", 1, HandlerFunc(func(ctx context.Context, d Delivery) error {
		<-gate
		return nil
	}), WithBackpressure(BackpressureConfig{PendingLimit: 1, MaxInFlight: 1, Policy: PolicyReject}))
	if err != nil {
		t.Fatal(err)
	}

	var rejected int
	var accepted int
	for i := 0; i < 6; i++ {
		id := MessageID(fmt.Sprintf("r-%d", i))
		_, err := b.Publish("orders", 1, map[string]any{"id": id, "amount": i}, WithMessageID(id))
		switch {
		case errors.Is(err, ErrBackpressure):
			rejected++
			t.Logf("publish id=%s rejected by backpressure (explicit, no silent drop)", id)
		case err != nil:
			t.Fatalf("unexpected err: %v", err)
		default:
			accepted++
			t.Logf("publish id=%s accepted", id)
		}
	}
	if rejected == 0 {
		t.Fatal("expected at least one explicit ErrBackpressure")
	}
	close(gate)
	// The accepted messages are durably in the log and all get delivered.
	end, _ := b.TopicEnd("orders")
	if int(end) != accepted {
		t.Fatalf("log end=%d, accepted=%d (rejected publishes must not corrupt log)", end, accepted)
	}
	waitCommit(t, sub, end-1)
}

// TestConcurrentAckAccounting: many goroutines ack/nack the same offsets;
// accounting must never double-count or panic.
func TestConcurrentAckAccounting(t *testing.T) {
	reg := testRegistry(t)
	b := NewBroker(reg, DefaultBrokerConfig())
	defer b.Close()

	// Handler auto-acks; additionally hammer Ack with duplicates and bad
	// offsets from outside, expecting stable sentinel errors.
	sub, err := b.Subscribe("orders", 1, HandlerFunc(func(ctx context.Context, d Delivery) error {
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	publishN(t, b, "orders", 20)
	waitCommit(t, sub, 19)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for off := Offset(0); off < 25; off++ {
				err := sub.Ack(off)
				switch {
				case err == nil, errors.Is(err, ErrAlreadyAcked), errors.Is(err, ErrBadAck):
				default:
					t.Errorf("unexpected ack err: %v", err)
				}
			}
		}(g)
	}
	wg.Wait()
	if sub.CommitPosition() != 19 {
		t.Fatalf("commit=%d want 19", sub.CommitPosition())
	}
}
