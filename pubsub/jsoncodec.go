package pubsub

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// JSONField describes one field of a JSON message schema.
type JSONField struct {
	Name     string
	Required bool
}

// JSONCodec is a strict JSON codec for one concrete schema version.
//
// Strictness is deliberate and supports the "no silent skipping" contract:
//   - a field declared Required but absent (or null) -> RejectMissingField;
//   - a payload field not declared in Fields         -> RejectUnknownField;
//   - payload that is not a JSON object or malformed -> RejectMalformed.
//
// Optional declared fields may be absent. Values are decoded into
// map[string]any so migrators and application code share one representation.
type JSONCodec struct {
	version SchemaVersion
	fields  map[string]JSONField
}

// NewJSONCodec registers a strict JSON schema of the given version.
func NewJSONCodec(version SchemaVersion, fields []JSONField) *JSONCodec {
	c := &JSONCodec{version: version, fields: make(map[string]JSONField, len(fields))}
	for _, f := range fields {
		c.fields[f.Name] = f
	}
	return c
}

// Version implements Codec.
func (c *JSONCodec) Version() SchemaVersion { return c.version }

// Encode implements Codec. It rejects attempts to encode an undeclared field
// and missing required fields symmetrically with Decode.
func (c *JSONCodec) Encode(v any) ([]byte, error) {
	m, err := asStringMap(v)
	if err != nil {
		return nil, &RejectError{Reason: RejectMalformed, FromVersion: c.version, Detail: err.Error()}
	}
	for name, f := range c.fields {
		if f.Required {
			if val, ok := m[name]; !ok || val == nil {
				return nil, &RejectError{Reason: RejectMissingField, FromVersion: c.version,
					Field: name, Detail: "required field missing while encoding"}
			}
		}
	}
	for name := range m {
		if _, ok := c.fields[name]; !ok {
			return nil, &RejectError{Reason: RejectUnknownField, FromVersion: c.version,
				Field: name, Detail: "field not declared by schema while encoding"}
		}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(m); err != nil {
		return nil, &RejectError{Reason: RejectMalformed, FromVersion: c.version, Detail: err.Error()}
	}
	return bytes.TrimSpace(buf.Bytes()), nil
}

// Decode implements Codec with the strict rules documented on JSONCodec.
func (c *JSONCodec) Decode(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, &RejectError{Reason: RejectMalformed, FromVersion: c.version, Detail: err.Error()}
	}
	if dec.More() {
		return nil, &RejectError{Reason: RejectMalformed, FromVersion: c.version,
			Detail: "payload contains multiple JSON values"}
	}
	for name, f := range c.fields {
		if f.Required {
			val, ok := m[name]
			if !ok || val == nil {
				return nil, &RejectError{Reason: RejectMissingField, FromVersion: c.version, Field: name}
			}
		}
	}
	for name := range m {
		if _, ok := c.fields[name]; !ok {
			return nil, &RejectError{Reason: RejectUnknownField, FromVersion: c.version, Field: name,
				Detail: fmt.Sprintf("field %q is not declared by schema version %d", name, c.version)}
		}
	}
	return m, nil
}

func asStringMap(v any) (map[string]any, error) {
	switch m := v.(type) {
	case map[string]any:
		return m, nil
	case *map[string]any:
		return *m, nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		var out map[string]any
		if err := json.Unmarshal(b, &out); err != nil {
			return nil, err
		}
		return out, nil
	}
}
