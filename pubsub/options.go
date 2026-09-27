package pubsub

import (
	"log"
	"os"
)

// PublishOption customises a publish call.
type PublishOption func(*publishOptions)

type publishOptions struct {
	id      MessageID
	headers map[string]string
}

// WithMessageID pins the message ID (otherwise one is generated). Publishers
// retrying a publish should use the same ID so subscribers can dedupe.
func WithMessageID(id MessageID) PublishOption {
	return func(o *publishOptions) { o.id = id }
}

// WithHeaders attaches headers to the published message.
func WithHeaders(h map[string]string) PublishOption {
	return func(o *publishOptions) { o.headers = h }
}

// SubOption customises a subscription.
type SubOption func(*subOptions)

type subOptions struct {
	name        string
	startOffset Offset
	latest      bool
	bp          BackpressureConfig
	rejecter    RejectHandler
}

// WithSubscriptionName gives a durable subscription a name. Re-attaching with
// the same name (in the same broker) resumes from the last acknowledged offset.
func WithSubscriptionName(name string) SubOption {
	return func(o *subOptions) { o.name = name }
}

// WithStartOffset starts a NEW subscription at an explicit offset. It has no
// effect when re-attaching a named subscription that already has a cursor.
func WithStartOffset(off Offset) SubOption {
	return func(o *subOptions) { o.startOffset = off; o.latest = false }
}

// WithStartLatest starts a new subscription after messages already present.
func WithStartLatest() SubOption {
	return func(o *subOptions) { o.latest = true }
}

// WithBackpressure overrides backpressure settings for one subscription.
func WithBackpressure(bp BackpressureConfig) SubOption {
	return func(o *subOptions) { o.bp = bp }
}

// WithRejectHandler installs the handler invoked for messages that cannot be
// decoded at the subscriber's schema version. Without it the subscription
// parks at the rejected offset (see Subscription.BlockedOn).
func WithRejectHandler(rh RejectHandler) SubOption {
	return func(o *subOptions) { o.rejecter = rh }
}

// Logger receives diagnostic events. It intentionally surfaces message IDs
// and offsets/commits so test logs and incident reports can reconstruct
// exactly what was delivered and where a subscription resumed.
type Logger interface {
	Delivered(topic string, id MessageID, off Offset, attempt int)
	Redelivered(topic string, id MessageID, off Offset, attempt int)
	Acked(topic string, name string, off Offset, commit Offset)
	Rejected(topic string, id MessageID, off Offset, reason RejectReason)
}

type nopLogger struct{}

func (nopLogger) Delivered(string, MessageID, Offset, int)         {}
func (nopLogger) Redelivered(string, MessageID, Offset, int)       {}
func (nopLogger) Acked(string, string, Offset, Offset)             {}
func (nopLogger) Rejected(string, MessageID, Offset, RejectReason) {}

// stdLogger writes one formatted line per event to a *log.Logger.
type stdLogger struct{ l *log.Logger }

// NewStdLogger wraps the standard library logger (nil = stderr).
func NewStdLogger(l *log.Logger) Logger {
	if l == nil {
		l = log.New(os.Stderr, "pubsub ", log.LstdFlags|log.Lmicroseconds)
	}
	return stdLogger{l: l}
}

func (s stdLogger) Delivered(topic string, id MessageID, off Offset, attempt int) {
	s.l.Printf("delivered  topic=%s id=%s offset=%d attempt=%d", topic, id, off, attempt)
}
func (s stdLogger) Redelivered(topic string, id MessageID, off Offset, attempt int) {
	s.l.Printf("redeliver  topic=%s id=%s offset=%d attempt=%d", topic, id, off, attempt)
}
func (s stdLogger) Acked(topic, name string, off, commit Offset) {
	s.l.Printf("acked      topic=%s sub=%s offset=%d commit=%d", topic, name, off, commit)
}
func (s stdLogger) Rejected(topic string, id MessageID, off Offset, reason RejectReason) {
	s.l.Printf("rejected   topic=%s id=%s offset=%d reason=%s", topic, id, off, reason)
}
