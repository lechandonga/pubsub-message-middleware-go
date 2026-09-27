package pubsub

import (
	"context"
	"testing"
	"time"
)

func newSmokeBroker(t testing.TB) (*Broker, *Registry) {
	t.Helper()
	reg := NewRegistry()
	c1 := NewJSONCodec(1, []JSONField{{Name: "text", Required: true}})
	reg.Register("t1", c1)
	b := NewBroker(reg, DefaultBrokerConfig())
	t.Cleanup(func() { _ = b.Close() })
	return b, reg
}

func TestSmokePubSub(t *testing.T) {
	b, _ := newSmokeBroker(t)
	got := make(chan MessageID, 4)
	sub, err := b.Subscribe("t1", 1, HandlerFunc(func(ctx context.Context, d Delivery) error {
		got <- d.Message.ID
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := b.Publish("t1", 1, map[string]any{"text": "hi"}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		select {
		case <-got:
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}
	waitCommit(t, sub, 2)
}

func waitCommit(t *testing.T, s *Subscription, want Offset) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.CommitPosition() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("commit=%d want %d", s.CommitPosition(), want)
}
