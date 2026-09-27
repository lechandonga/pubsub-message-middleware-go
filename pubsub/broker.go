package pubsub

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// Broker is the local in-process message middleware.
//
// Delivery semantics (see docs/design.md for the full contract):
//
//   - Per topic, messages form a dense, monotonic offset log starting at 0.
//   - Delivery is at-least-once: a message is redelivered (Attempt incremented)
//     until the contiguous head is Acked. Commit advances only in order, so a
//     gap can never be silently skipped.
//   - Detach/Attach or re-subscribing with the same durable name resumes at
//     commit+1. Redelivered messages keep the same Message.ID, so duplicates
//     are identifiable (see DedupTracker).
type Broker struct {
	mu     sync.RWMutex
	cfg    BrokerConfig
	reg    *Registry
	topics map[string]*topicLog
	closed bool

	closeCh chan struct{} // closed once on Broker.Close

	idSeq  atomic.Int64
	logger Logger
}

// NewBroker creates a broker with the given registry and configuration.
func NewBroker(reg *Registry, cfg BrokerConfig) *Broker {
	b := &Broker{
		cfg:    cfg,
		reg:    reg,
		topics: make(map[string]*topicLog),
		logger: cfg.Logger,
	}
	if b.logger == nil {
		b.logger = nopLogger{}
	}
	b.closeCh = make(chan struct{})
	return b
}

// Publish appends one message to a topic and returns it with its assigned
// offset. Admission (backpressure) is decided BEFORE the message enters the
// log, while holding the topic's serialized admission lock:
//
//   - PolicyReject: if the slowest active subscriber would exceed
//     PendingLimit, Publish returns ErrBackpressure and stores nothing;
//   - PolicyBlock:   Publish waits (releasing internal locks so acks still
//     make progress) until the subscriber acknowledges, then re-checks. No
//     message is ever silently dropped.
//
// Detached (offline) subscribers do not exert backpressure; messages
// accumulate in the log and are delivered on re-attach from commit+1.
func (b *Broker) Publish(topic string, v SchemaVersion, payload any, opts ...PublishOption) (*Message, error) {
	o := &publishOptions{}
	for _, opt := range opts {
		opt(o)
	}
	data, err := b.reg.Encode(topic, v, payload)
	if err != nil {
		return nil, err
	}
	id := o.id
	if id == "" {
		id = MessageID(fmt.Sprintf("m-%d", b.idSeq.Add(1)))
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrClosed
	}
	t, ok := b.topics[topic]
	b.mu.RUnlock()
	if !ok {
		if !b.cfg.AutoCreateTopics {
			return nil, ErrUnknownTopic
		}
		t = b.createTopic(topic)
	}

	// Serialize admission across publishers of this topic so backpressure is
	// fair and bounded (a late, not-yet-full publisher cannot jump the queue).
	t.admMu.Lock()
	defer t.admMu.Unlock()
	for {
		t.mu.Lock()
		decision, limit, lag := b.evaluateLocked(t)
		if decision == admitAllow {
			msg := &Message{
				Topic:         topic,
				ID:            id,
				SchemaVersion: v,
				Headers:       cloneHeaders(o.headers),
				Data:          data,
				Attempt:       1,
			}
			t.appendLocked(msg)
			t.mu.Unlock()
			b.kickSubscribers(t)
			return msg, nil
		}
		if decision == admitReject {
			t.mu.Unlock()
			return nil, fmt.Errorf("%w: topic=%q lag=%d limit=%d (PolicyReject)",
				ErrBackpressure, topic, lag, limit)
		}
		// admitBlock. We hold t.admMu (serializing publishers) and t.mu here.
		// Release the log lock so acks (which take topic.mu only to write the
		// cursor) can make progress, then wait on the condition IN THIS
		// GOROUTINE — Cond.Wait requires the same goroutine to hold admMu,
		// atomically release it, and be enqueued before any broadcast.
		t.mu.Unlock()
		t.admCond.Wait() // releases admMu, reacquires on wake
		if b.closedNow() {
			return nil, ErrClosed // deferred t.admMu.Unlock releases the mutex
		}
	}
}

func (b *Broker) closedNow() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.closed
}

func (b *Broker) createTopic(name string) *topicLog {
	b.mu.Lock()
	defer b.mu.Unlock()
	if t, ok := b.topics[name]; ok {
		return t
	}
	t := newTopicLog(name, b.cfg.Retention)
	b.topics[name] = t
	return t
}

type admitDecision int

const (
	admitAllow admitDecision = iota
	admitBlock
	admitReject
)

// evaluateLocked applies the deterministic admission rule against every ACTIVE
// subscription. Detached (offline) subscribers do NOT exert backpressure:
// messages accumulate in the log until they resume.
//
// Pending for a subscriber = end - commit: how many messages would be
// uncommitted for it after the candidate message is admitted (commit+1..end).
// Reject takes precedence over block, so mixed per-subscription policies fail
// fast deterministically.
//
// Caller holds t.mu; Publish additionally holds t.admMu, which serializes
// publishers so the check plus the subsequent append are one atomic admission.
func (b *Broker) evaluateLocked(t *topicLog) (admitDecision, int, int) {
	end := t.lengthLocked()
	decision := admitAllow
	worstLimit, worstLag := 0, 0
	for s := range t.subs {
		s.mu.Lock()
		active := s.attached && !s.closed
		commit := s.commit
		limit := s.bp.PendingLimit
		policy := s.bp.Policy
		s.mu.Unlock()
		if !active {
			continue
		}
		pending := int(end - commit)
		if pending <= limit {
			continue
		}
		if policy == PolicyReject {
			return admitReject, limit, pending
		}
		decision = admitBlock
		worstLimit, worstLag = limit, pending
	}
	return decision, worstLimit, worstLag
}

func (b *Broker) kickSubscribers(t *topicLog) {
	t.mu.Lock()
	subs := make([]*subState, 0, len(t.subs))
	for s := range t.subs {
		subs = append(subs, s)
	}
	t.mu.Unlock()
	for _, s := range subs {
		s.kick()
	}
}

// Subscribe attaches a handler to a topic.
//
// Named subscriptions (WithSubscriptionName) are durable within the broker:
// re-subscribing with the same name resumes from the last committed offset+1,
// even across Detach/Close of the previous handle. Unnamed subscriptions
// start at WithStartOffset (default 0) or WithStartLatest.
func (b *Broker) Subscribe(topic string, wantVersion SchemaVersion, h Handler, opts ...SubOption) (*Subscription, error) {
	o := &subOptions{bp: b.cfg.Backpressure, startOffset: 0}
	for _, opt := range opts {
		opt(o)
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, ErrClosed
	}
	t, ok := b.topics[topic]
	b.mu.RUnlock()
	if !ok {
		if !b.cfg.AutoCreateTopics {
			return nil, ErrUnknownTopic
		}
		t = b.createTopic(topic)
	}

	var rh RejectHandler
	if o.rejecter != nil {
		rh = o.rejecter
	} else if rhOpt, ok := h.(RejectHandler); ok {
		rh = rhOpt
	}

	bp := o.bp.withDefaults()
	s := newSubState(b, t, o.name, wantVersion, h, rh, bp)

	t.mu.Lock()
	defer t.mu.Unlock()
	var start Offset
	if o.name != "" {
		if c, ok := t.cursorLocked(o.name); ok {
			start = c + 1 // resume
		} else if o.latest {
			start = t.lengthLocked()
			t.setCursorLocked(o.name, start-1)
		} else {
			start = o.startOffset
			t.setCursorLocked(o.name, start-1)
		}
	} else if o.latest {
		start = t.lengthLocked()
	} else {
		start = o.startOffset
	}
	t.addSubLocked(s)
	s.startLocked(start)
	return &Subscription{sub: s}, nil
}

// closeSub detaches and unregisters a subscription. The durable cursor of a
// named subscription remains in the topic log.
func (b *Broker) closeSub(s *subState) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.attached = false
	stop := s.stopCh
	t := s.topic
	cancel := s.cancel
	s.kickLocked()
	s.mu.Unlock()
	close(stop)
	cancel() // unblock handlers/rejecters honouring Delivery.Context()

	t.mu.Lock()
	t.removeSubLocked(s)
	t.mu.Unlock()
	// Removing a subscriber can unblock publishers; re-evaluate everyone.
	t.admCond.Broadcast()

	s.wg.Wait()  // dispatcher exited
	s.hwg.Wait() // in-flight handlers finished
	return nil
}

// Close shuts the broker down. Subsequent publishes and acks fail with
// ErrClosed; named cursors remain only in memory for the broker's lifetime.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	topics := b.topics
	b.mu.Unlock()
	close(b.closeCh)
	for _, t := range topics {
		t.admCond.Broadcast()
	}

	for _, t := range topics {
		t.mu.Lock()
		subs := make([]*subState, 0, len(t.subs))
		for s := range t.subs {
			subs = append(subs, s)
		}
		t.subs = make(map[*subState]struct{})
		t.mu.Unlock()
		for _, s := range subs {
			_ = b.closeSub(s)
		}
	}
	return nil
}

// TopicEnd returns the exclusive end offset of a topic (for tests/tooling).
func (b *Broker) TopicEnd(topic string) (Offset, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.topics[topic]
	if !ok {
		return 0, ErrUnknownTopic
	}
	return t.lengthEnd(), nil
}

func cloneHeaders(h map[string]string) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, v := range h {
		out[k] = v
	}
	return out
}
