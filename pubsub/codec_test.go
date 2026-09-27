package pubsub

import (
	"errors"
	"testing"
)

var (
	testKnown    = []string{"id", "amount", "currency"}
	testRequired = []string{"id", "amount"}
)

func TestCodecRoundTrip(t *testing.T) {
	c := NewCodec(1, 2)
	data, err := c.Encode(2, map[string]any{"id": "m-1", "amount": 42, "currency": "CNY"})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	ver, body, err := c.Decode(data, testRequired, testKnown)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if ver != 2 {
		t.Fatalf("version = %d, want 2", ver)
	}
	if body["id"] != "m-1" {
		t.Fatalf("body[id] = %v", body["id"])
	}
	t.Logf("roundtrip ok: version=%d body=%v", ver, body)
}

func TestCodecVersionMismatch(t *testing.T) {
	c := NewCodec(1, 2)
	// 编码方用 v3，超出接受范围 [1,2]
	if _, err := c.Encode(3, map[string]any{"id": "m-2", "amount": 1}); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("Encode v3 err = %v, want ErrVersionMismatch", err)
	}
	// 构造一个 v0 的报文直接解码
	legacy := NewCodec(0, 0)
	data, err := legacy.Encode(0, map[string]any{"id": "m-3", "amount": 1})
	if err != nil {
		t.Fatalf("legacy Encode: %v", err)
	}
	if _, _, err := c.Decode(data, testRequired, testKnown); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("Decode v0 err = %v, want ErrVersionMismatch", err)
	}
	t.Logf("version mismatch rejected with distinguishable reason")
}

func TestCodecUnknownField(t *testing.T) {
	c := NewCodec(1, 1)
	data, err := c.Encode(1, map[string]any{"id": "m-4", "amount": 1, "surprise": "x"})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	_, _, err = c.Decode(data, testRequired, testKnown)
	if !errors.Is(err, ErrUnknownField) {
		t.Fatalf("Decode err = %v, want ErrUnknownField", err)
	}
	// 不能与别的原因混淆
	if errors.Is(err, ErrMissingField) || errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("reason confused: %v", err)
	}
	t.Logf("unknown field rejected: %v", err)
}

func TestCodecMissingField(t *testing.T) {
	c := NewCodec(1, 1)
	data, err := c.Encode(1, map[string]any{"currency": "CNY"})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	_, _, err = c.Decode(data, testRequired, testKnown)
	if !errors.Is(err, ErrMissingField) {
		t.Fatalf("Decode err = %v, want ErrMissingField", err)
	}
	if errors.Is(err, ErrUnknownField) || errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("reason confused: %v", err)
	}
	t.Logf("missing field rejected: %v", err)
}
