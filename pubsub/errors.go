package pubsub

import (
	"errors"
	"fmt"
)

// RejectReason categorizes why a message could not be decoded or delivered.
// Reasons are stable strings so callers can branch on them.
type RejectReason string

const (
	// RejectSchemaMissing means the carried schema version is zero/unknown.
	RejectSchemaMissing RejectReason = "schema_version_missing"
	// RejectSchemaUnknown means no codec is registered for the carried version.
	RejectSchemaUnknown RejectReason = "schema_version_unknown"
	// RejectIncompatibleVersion means the message version cannot be converted
	// to the version the subscriber requested (e.g. newer than supported and
	// no forward migration is registered).
	RejectIncompatibleVersion RejectReason = "incompatible_version"
	// RejectMissingField means a required field is absent in the payload.
	RejectMissingField RejectReason = "missing_required_field"
	// RejectUnknownField means the payload carries a field the target schema
	// does not declare.
	RejectUnknownField RejectReason = "unknown_field"
	// RejectMalformed means the payload bytes are not valid for the codec.
	RejectMalformed RejectReason = "malformed_payload"
)

// RejectError describes a decode/compatibility rejection with a stable reason.
// It is returned instead of silently dropping a message so that callers can
// tell apart missing fields, unknown fields and version mismatches.
type RejectError struct {
	Reason      RejectReason
	Topic       string
	ID          MessageID
	FromVersion SchemaVersion
	WantVersion SchemaVersion
	Field       string
	Detail      string
}

func (e *RejectError) Error() string {
	s := fmt.Sprintf("pubsub: message rejected: reason=%s topic=%q id=%q", e.Reason, e.Topic, e.ID)
	if e.FromVersion != 0 || e.WantVersion != 0 {
		s += fmt.Sprintf(" version=%d->%d", e.FromVersion, e.WantVersion)
	}
	if e.Field != "" {
		s += " field=" + e.Field
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

// AsRejectError extracts a *RejectError from err if present.
func AsRejectError(err error) (*RejectError, bool) {
	var re *RejectError
	if errors.As(err, &re) {
		return re, true
	}
	return nil, false
}

var (
	// ErrNotAttached is returned when an operation requires an attached subscriber.
	ErrNotAttached = errors.New("pubsub: subscriber is not attached")
	// ErrClosed is returned after the broker (or subscription) is closed.
	ErrClosed = errors.New("pubsub: broker closed")
	// ErrUnknownTopic is returned when a topic does not exist and auto-create is off.
	ErrUnknownTopic = errors.New("pubsub: unknown topic")
	// ErrBackpressure is returned when a publish/dispatch is rejected by policy.
	ErrBackpressure = errors.New("pubsub: backpressure limit exceeded")
	// ErrBadAck is returned when an acknowledgement does not refer to a valid
	// in-flight or previously delivered offset.
	ErrBadAck = errors.New("pubsub: invalid acknowledgement")
	// ErrAlreadyAcked is returned when an offset is acknowledged twice.
	ErrAlreadyAcked = errors.New("pubsub: offset already acknowledged")
)
