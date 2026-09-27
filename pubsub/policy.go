package pubsub

// OverflowPolicy decides what happens when a subscriber is slower than the
// publisher: its redelivery buffer (messages behind the unacknowledged head)
// is full.
type OverflowPolicy int

const (
	// PolicyBlock makes dispatch/publish block until the slow subscriber
	// acknowledges and makes room. This never silently drops data; backpressure
	// propagates deterministically to producers.
	PolicyBlock OverflowPolicy = iota
	// PolicyReject makes the publish fail with ErrBackpressure instead of
	// dropping the message. The producer sees an explicit, retryable failure.
	PolicyReject
)

func (p OverflowPolicy) String() string {
	switch p {
	case PolicyBlock:
		return "block"
	case PolicyReject:
		return "reject"
	default:
		return "unknown"
	}
}

// Default pending buffer and inflight values.
const (
	defaultPendingLimit = 1024
	defaultMaxInFlight  = 1
)

// BackpressureConfig tunes per-subscriber buffering and concurrency limits.
type BackpressureConfig struct {
	// PendingLimit is the maximum number of messages buffered for one slow
	// subscriber behind its unacknowledged head. 0 means defaultPendingLimit.
	PendingLimit int
	// MaxInFlight is the maximum number of concurrently dispatched but
	// unacknowledged messages per subscription. 1 (the default) enforces
	// strict offset-ordered, resumable delivery; higher values allow
	// concurrent handlers at the cost of per-message ordering.
	MaxInFlight int
	// Policy selects what happens when PendingLimit is exceeded.
	Policy OverflowPolicy
}

// DefaultBackpressureConfig returns the conservative default configuration:
// block slow publishers, one in-flight message per subscriber.
func DefaultBackpressureConfig() BackpressureConfig {
	return BackpressureConfig{
		PendingLimit: defaultPendingLimit,
		MaxInFlight:  defaultMaxInFlight,
		Policy:       PolicyBlock,
	}
}

func (b BackpressureConfig) withDefaults() BackpressureConfig {
	d := DefaultBackpressureConfig()
	if b.PendingLimit <= 0 {
		b.PendingLimit = d.PendingLimit
	}
	if b.MaxInFlight <= 0 {
		b.MaxInFlight = d.MaxInFlight
	}
	return b
}
