package pubsub

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestJSONCodecStrict(t *testing.T) {
	c := NewJSONCodec(1, []JSONField{
		{Name: "id", Required: true},
		{Name: "note", Required: false},
	})
	t.Run("ok", func(t *testing.T) {
		v, err := c.Decode([]byte(`{"id":"x","note":"y"}`))
		if err != nil {
			t.Fatal(err)
		}
		if v.(map[string]any)["id"] != "x" {
			t.Fatal("bad decode")
		}
	})
	t.Run("missing_required", func(t *testing.T) {
		_, err := c.Decode([]byte(`{"note":"y"}`))
		mustReject(t, err, RejectMissingField, "id")
	})
	t.Run("unknown_field", func(t *testing.T) {
		_, err := c.Decode([]byte(`{"id":"x","bogus":1}`))
		mustReject(t, err, RejectUnknownField, "bogus")
	})
	t.Run("malformed", func(t *testing.T) {
		_, err := c.Decode([]byte(`{not json`))
		mustReject(t, err, RejectMalformed, "")
	})
}

func mustReject(t *testing.T, err error, reason RejectReason, field string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want rejection %s, got nil", reason)
	}
	var re *RejectError
	if !errors.As(err, &re) {
		t.Fatalf("want *RejectError, got %T: %v", err, err)
	}
	if re.Reason != reason {
		t.Fatalf("reason = %s, want %s (%v)", re.Reason, reason, err)
	}
	if field != "" && re.Field != field {
		t.Fatalf("field = %q, want %q", re.Field, field)
	}
	t.Logf("rejected as expected: %v", err)
}

func TestRegistryVersionRules(t *testing.T) {
	reg := NewRegistry()
	c1 := NewJSONCodec(1, []JSONField{{Name: "id", Required: true}})
	c2 := NewJSONCodec(2, []JSONField{
		{Name: "id", Required: true},
		{Name: "currency", Required: false},
	})
	reg.Register("ev", c1)
	reg.Register("ev", c2)
	reg.RegisterMigrator("ev", 1, 2, func(v any) (any, error) {
		m := v.(map[string]any)
		m["currency"] = "USD"
		return m, nil
	})

	// Same version decodes directly.
	if _, err := reg.Decode("ev", 1, []byte(`{"id":"a"}`), 1); err != nil {
		t.Fatalf("same-version decode: %v", err)
	}

	// Old message -> newer subscriber: explicit up-migrator applied.
	v, err := reg.Decode("ev", 2, []byte(`{"id":"a"}`), 1)
	if err != nil {
		t.Fatalf("up-conversion: %v", err)
	}
	if v.(map[string]any)["currency"] != "USD" {
		t.Fatalf("migrator did not run: %#v", v)
	}

	// Newer message -> older subscriber with no down-migrator is a DISTINCT
	// rejection, not a silent skip.
	_, err = reg.Decode("ev", 1, []byte(`{"id":"a","currency":"EUR"}`), 2)
	mustReject(t, err, RejectIncompatibleVersion, "")

	// Version zero is never implicitly assumed.
	_, err = reg.Decode("ev", 1, []byte(`{"id":"a"}`), 0)
	mustReject(t, err, RejectSchemaMissing, "")

	// Unknown carried version.
	_, err = reg.Decode("ev", 1, []byte(`{"id":"a"}`), 9)
	mustReject(t, err, RejectSchemaUnknown, "")

	// Missing required field inside an otherwise-compatible message.
	_, err = reg.Decode("ev", 1, []byte(`{}`), 1)
	mustReject(t, err, RejectMissingField, "id")

	// Unknown field inside an otherwise-compatible message.
	_, err = reg.Decode("ev", 1, []byte(`{"id":"a","x":1}`), 1)
	mustReject(t, err, RejectUnknownField, "x")

	// Explicit down-migrator makes new->old compatible.
	reg.RegisterMigrator("ev", 2, 1, func(v any) (any, error) {
		m := v.(map[string]any)
		delete(m, "currency")
		return m, nil
	})
	if _, err := reg.Decode("ev", 1, []byte(`{"id":"a","currency":"EUR"}`), 2); err != nil {
		t.Fatalf("down-conversion with explicit migrator: %v", err)
	}
}

// TestIncompatibleMessageParksThenResolves: a v3 message that a v1
// subscriber cannot decode must not be skipped; delivery parks at that offset
// with a classifiable cause and resumes after the application explicitly acks
// it (e.g. after quarantining).
func TestIncompatibleMessageParksThenResolves(t *testing.T) {
	reg := NewRegistry()
	reg.Register("ev", NewJSONCodec(1, []JSONField{{Name: "id", Required: true}}))
	reg.Register("ev", NewJSONCodec(3, []JSONField{{Name: "id", Required: true}}))
	b := NewBroker(reg, BrokerConfig{AutoCreateTopics: true, Logger: NewStdLogger(nil)})
	defer b.Close()

	processed := make(chan MessageID, 4)
	sub, err := b.Subscribe("ev", 1, HandlerFunc(func(ctx context.Context, d Delivery) error {
		processed <- d.Message.ID
		return nil
	}), WithRejectHandler(RejectHandlerFunc(func(ctx context.Context, d Delivery, cause error) error {
		re, ok := AsRejectError(cause)
		if !ok || re.Reason != RejectIncompatibleVersion {
			t.Errorf("want incompatible_version, got %v", cause)
			return d.Nack()
		}
		t.Logf("reject id=%s offset=%d reason=%s", d.Message.ID, d.Message.Offset, re.Reason)
		// Application quarantines then explicitly acknowledges, unblocking
		// delivery without ever silently skipping.
		return d.Ack()
	})))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := b.Publish("ev", 1, map[string]any{"id": "ok-0"}, WithMessageID("ok-0")); err != nil {
		t.Fatal(err)
	}
	waitID(t, processed, "ok-0", time.Second)

	if _, err := b.Publish("ev", 3, map[string]any{"id": "future-1"}, WithMessageID("future-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish("ev", 1, map[string]any{"id": "ok-2"}, WithMessageID("ok-2")); err != nil {
		t.Fatal(err)
	}
	// ok-2 must still be delivered after the parked message is explicitly acked.
	waitID(t, processed, "ok-2", 2*time.Second)
	waitCommit(t, sub, 2)
}

func waitID(t *testing.T, ch <-chan MessageID, want MessageID, d time.Duration) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("got id %s, want %s", got, want)
		}
	case <-time.After(d):
		t.Fatalf("timeout waiting for id %s", want)
	}
}
