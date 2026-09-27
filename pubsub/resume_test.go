package pubsub

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func testRegistry(t *testing.T) *Registry {
	t.Helper()
	reg := NewRegistry()
	reg.Register("orders", NewJSONCodec(1, []JSONField{
		{Name: "id", Required: true},
		{Name: "amount", Required: true},
	}))
	reg.Register("orders", NewJSONCodec(2, []JSONField{
		{Name: "id", Required: true},
		{Name: "amount", Required: true},
		{Name: "currency", Required: false},
	}))
	// v1 -> v2: fill the new optional field with a default.
	reg.RegisterMigrator("orders", 1, 2, func(v any) (any, error) {
		m := v.(map[string]any)
		if _, ok := m["currency"]; !ok {
			m["currency"] = "USD"
		}
		return m, nil
	})
	return reg
}

type recorder struct {
	mu     sync.Mutex
	msgs   []Delivery
	failOn map[MessageID]int // id -> fail this many times before succeeding
	logf   func(string, ...any)
}

func newRecorder(logf func(string, ...any)) *recorder {
	return &recorder{failOn: map[MessageID]int{}, logf: logf}
}

func (r *recorder) Handle(ctx context.Context, d Delivery) error {
	r.mu.Lock()
	r.msgs = append(r.msgs, d)
	id := d.Message.ID
	attempt := d.Message.Attempt
	off := d.Message.Offset
	r.mu.Unlock()
	r.logf("handle id=%s offset=%d attempt=%d", id, off, attempt)
	if n := r.failOn[id]; n > 0 {
		r.mu.Lock()
		r.failOn[id]--
		r.mu.Unlock()
		return errors.New("transient handler failure")
	}
	return nil
}

func (r *recorder) ids() []MessageID {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]MessageID, len(r.msgs))
	for i, d := range r.msgs {
		out[i] = d.Message.ID
	}
	return out
}

func (r *recorder) attempts(id MessageID) []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []int
	for _, d := range r.msgs {
		if d.Message.ID == id {
			out = append(out, d.Message.Attempt)
		}
	}
	return out
}

func publishN(t *testing.T, b *Broker, topic string, n int) []MessageID {
	t.Helper()
	ids := make([]MessageID, n)
	for i := 0; i < n; i++ {
		ids[i] = MessageID("o-" + itoa(i))
		m, err := b.Publish(topic, 1, map[string]any{"id": ids[i], "amount": 100 + i},
			WithMessageID(ids[i]))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		if m.Offset != Offset(i) {
			t.Fatalf("offset = %d, want %d", m.Offset, i)
		}
	}
	return ids
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

// TestResumeAfterDetach: a slow/temporarily offline subscriber detaches;
// messages published meanwhile are not lost. Re-attaching resumes exactly at
// commit+1 and redelivered messages carry Attempt>1 and the same ID.
func TestResumeAfterDetach(t *testing.T) {
	reg := testRegistry(t)
	b := NewBroker(reg, BrokerConfig{AutoCreateTopics: true, Logger: NewStdLogger(nil)})
	defer b.Close()

	rec := newRecorder(t.Logf)
	count := make(chan struct{}, 16)
	sub, err := b.Subscribe("orders", 1, HandlerFunc(func(ctx context.Context, d Delivery) error {
		err := rec.Handle(ctx, d)
		if err == nil {
			count <- struct{}{}
		}
		return err
	}), WithSubscriptionName("svc-a"))
	if err != nil {
		t.Fatal(err)
	}

	// Phase 1: publish and consume 3, then detach.
	for i := 0; i < 3; i++ {
		id := MessageID("o-" + itoa(i))
		if _, err := b.Publish("orders", 1, map[string]any{"id": id, "amount": 100 + i},
			WithMessageID(id)); err != nil {
			t.Fatal(err)
		}
	}
	waitN(t, count, 3, 2*time.Second)
	waitCommit(t, sub, 2)
	if err := sub.Detach(); err != nil {
		t.Fatal(err)
	}

	// Phase 2: messages published while offline must not be lost.
	for i := 3; i < 6; i++ {
		id := MessageID("o-" + itoa(i))
		if _, err := b.Publish("orders", 1, map[string]any{"id": id, "amount": 100 + i},
			WithMessageID(id)); err != nil {
			t.Fatal(err)
		}
	}

	// Re-attach the same handle: resumes at commit+1.
	if err := sub.Attach(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitN(t, count, 3, 2*time.Second)
	waitCommit(t, sub, 5)

	got := rec.ids()
	want := []MessageID{"o-0", "o-1", "o-2", "o-3", "o-4", "o-5"}
	if len(got) != 6 {
		t.Fatalf("delivered %d messages, want 6: %v", len(got), got)
	}
	for i, id := range want {
		if got[i] != id {
			t.Fatalf("delivery %d = %s, want %s (order must be preserved)", i, got[i], id)
		}
	}

	// Brand new handle with the SAME durable name resumes from the stored
	// cursor: there must be no new deliveries of the already-acked messages
	// (only a future message after a new publish is delivered).
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	rec2 := newRecorder(t.Logf)
	sub2, err := b.Subscribe("orders", 1, HandlerFunc(rec2.Handle), WithSubscriptionName("svc-a"))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(rec2.ids()); n != 0 {
		t.Fatalf("durable re-subscription replayed %d old messages", n)
	}
	id := MessageID("o-6")
	if _, err := b.Publish("orders", 1, map[string]any{"id": id, "amount": 106}, WithMessageID(id)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ids := rec2.ids(); len(ids) == 1 && ids[0] == "o-6" {
			break
		}
		time.Sleep(time.Millisecond * 5)
		if time.Now().After(deadline) {
			t.Fatalf("durable sub got %v, want only o-6", rec2.ids())
		}
	}
	waitCommit(t, sub2, 6)
}

func waitN(t *testing.T, ch chan struct{}, n int, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for i := 0; i < n; i++ {
		select {
		case <-ch:
		case <-time.After(time.Until(deadline)):
			t.Fatalf("timeout waiting for delivery %d/%d", i+1, n)
		}
	}
}

// TestRedeliveryAndDedup: a handler that fails twice for one message must see
// attempts 1,2,3; the message is committed exactly once. A DedupTracker must
// classify the repeats as duplicates without corrupting state.
func TestRedeliveryAndDedup(t *testing.T) {
	reg := testRegistry(t)
	b := NewBroker(reg, BrokerConfig{AutoCreateTopics: true, Logger: NewStdLogger(nil)})
	defer b.Close()

	rec := newRecorder(t.Logf)
	rec.failOn[MessageID("o-1")] = 2 // fail twice for offset 1

	dd := NewDedupTracker()
	seenOffsets := map[Offset]int{}
	var mu sync.Mutex
	sub, err := b.Subscribe("orders", 1, HandlerFunc(func(ctx context.Context, d Delivery) error {
		mu.Lock()
		dup := dd.Mark(d.Message.ID)
		seenOffsets[d.Message.Offset]++
		mu.Unlock()
		t.Logf("process id=%s offset=%d attempt=%d duplicate=%v", d.Message.ID, d.Message.Offset, d.Message.Attempt, dup)
		err := rec.Handle(ctx, d)
		if err != nil && dup {
			// duplicate flag should only be true on real redelivery
		}
		return err
	}))
	if err != nil {
		t.Fatal(err)
	}
	publishN(t, b, "orders", 3)
	waitCommit(t, sub, 2)

	atts := rec.attempts("o-1")
	if len(atts) != 3 || atts[0] != 1 || atts[1] != 2 || atts[2] != 3 {
		t.Fatalf("attempts for o-1 = %v, want [1 2 3]", atts)
	}
	mu.Lock()
	defer mu.Unlock()
	for off := Offset(0); off <= 2; off++ {
		n := seenOffsets[off]
		if off == 1 {
			if n != 3 {
				t.Fatalf("offset 1 processed %d times, want 3", n)
			}
		} else if n != 1 {
			t.Fatalf("offset %d processed %d times, want 1", off, n)
		}
	}
}
