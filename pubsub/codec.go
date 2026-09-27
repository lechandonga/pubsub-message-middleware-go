package pubsub

import (
	"fmt"
	"sync"
)

// Codec encodes and decodes payloads of one concrete schema version.
//
// Implementations own their payload type. Decode must reject, rather than
// silently ignore, missing required fields and unknown fields, returning a
// *RejectError with an appropriate RejectReason.
type Codec interface {
	Version() SchemaVersion
	Encode(v any) ([]byte, error)
	Decode(data []byte) (any, error)
}

// Migrator converts a payload decoded at one schema version into the value
// shape expected by another schema version.
type Migrator func(v any) (any, error)

// Registry holds per-topic codecs, compatibility migrators and the rules
// describing how versions inter-operate.
type Registry struct {
	mu        sync.RWMutex
	codecs    map[string]map[SchemaVersion]Codec
	migrators map[string]map[[2]SchemaVersion]Migrator
}

// NewRegistry creates an empty codec registry.
func NewRegistry() *Registry {
	return &Registry{
		codecs:    make(map[string]map[SchemaVersion]Codec),
		migrators: make(map[string]map[[2]SchemaVersion]Migrator),
	}
}

// Register adds a codec for one topic/version.
func (r *Registry) Register(topic string, c Codec) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.codecs[topic]
	if m == nil {
		m = make(map[SchemaVersion]Codec)
		r.codecs[topic] = m
	}
	m[c.Version()] = c
}

// RegisterMigrator registers a converter from -> to for a topic. Migrators
// must be explicitly registered in both directions when both are intended:
// old->new up-conversion does not imply new->old down-conversion.
func (r *Registry) RegisterMigrator(topic string, from, to SchemaVersion, m Migrator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	mg := r.migrators[topic]
	if mg == nil {
		mg = make(map[[2]SchemaVersion]Migrator)
		r.migrators[topic] = mg
	}
	mg[[2]SchemaVersion{from, to}] = m
}

// Codec returns the codec registered for topic/version.
func (r *Registry) Codec(topic string, v SchemaVersion) (Codec, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if m, ok := r.codecs[topic]; ok {
		c, ok := m[v]
		return c, ok
	}
	return nil, false
}

// Encode encodes val at version v for a topic.
func (r *Registry) Encode(topic string, v SchemaVersion, val any) ([]byte, error) {
	c, ok := r.Codec(topic, v)
	if !ok {
		if v == 0 {
			return nil, &RejectError{Reason: RejectSchemaMissing, Topic: topic, WantVersion: v,
				Detail: "no codec registered: schema version is zero"}
		}
		return nil, &RejectError{Reason: RejectSchemaUnknown, Topic: topic, WantVersion: v,
			Detail: fmt.Sprintf("no codec registered for version %d", v)}
	}
	return c.Encode(val)
}

// Decode decodes data (carrying version v) into the shape requested by
// wantVersion for topic.
//
// Compatibility rules (deterministic):
//  1. v == 0 is rejected with RejectSchemaMissing (no implicit version).
//  2. No codec for v is rejected with RejectSchemaUnknown.
//  3. v == want: the payload is decoded directly by that codec. The codec is
//     responsible for RejectMissingField / RejectUnknownField / RejectMalformed.
//  4. v != want: a migrator must be explicitly registered for v->want. It is
//     applied after decoding. Absence is RejectIncompatibleVersion — the
//     message is never silently skipped or coerced.
func (r *Registry) Decode(topic string, want SchemaVersion, data []byte, v SchemaVersion) (any, error) {
	if v == 0 {
		return nil, &RejectError{Reason: RejectSchemaMissing, Topic: topic, WantVersion: want,
			Detail: "message carries no schema version"}
	}
	c, ok := r.Codec(topic, v)
	if !ok {
		return nil, &RejectError{Reason: RejectSchemaUnknown, Topic: topic, FromVersion: v, WantVersion: want,
			Detail: fmt.Sprintf("no codec registered for version %d", v)}
	}
	val, err := c.Decode(data)
	if err != nil {
		// Normalise codec errors into a RejectError carrying topic/versions.
		re, isReject := AsRejectError(err)
		if !isReject {
			return nil, &RejectError{Reason: RejectMalformed, Topic: topic, FromVersion: v,
				WantVersion: want, Detail: err.Error()}
		}
		if re.Topic == "" {
			re.Topic = topic
		}
		re.FromVersion = v
		re.WantVersion = want
		return nil, re
	}
	if v == want {
		return val, nil
	}
	m, ok := r.migrator(topic, v, want)
	if !ok {
		return nil, &RejectError{Reason: RejectIncompatibleVersion, Topic: topic,
			FromVersion: v, WantVersion: want,
			Detail: fmt.Sprintf("no migrator registered from version %d to %d", v, want)}
	}
	out, err := m(val)
	if err != nil {
		re, isReject := AsRejectError(err)
		if !isReject {
			return nil, &RejectError{Reason: RejectIncompatibleVersion, Topic: topic,
				FromVersion: v, WantVersion: want, Detail: err.Error()}
		}
		if re.Topic == "" {
			re.Topic = topic
		}
		re.FromVersion = v
		re.WantVersion = want
		return nil, re
	}
	return out, nil
}

func (r *Registry) migrator(topic string, from, to SchemaVersion) (Migrator, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if mg, ok := r.migrators[topic]; ok {
		m, ok := mg[[2]SchemaVersion{from, to}]
		return m, ok
	}
	return nil, false
}
