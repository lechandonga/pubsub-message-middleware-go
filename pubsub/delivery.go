package pubsub

import "context"

// Delivery is one dispatch of a message to a subscriber.
type Delivery struct {
	// Message is the stored envelope. Attempt is >= 1; values > 1 signal a
	// redelivery of the same message ID (so duplicates are recognizable).
	Message *Message
	// Payload is the codec-decoded (and, if needed, migrated) value at the
	// subscriber's requested schema version.
	Payload any
	sub     *subState
	ctx     context.Context
}

// Context returns a context cancelled when the subscription is Detached or
// Closed. Handlers should respect it so an offline/closed subscriber does not
// leave a handler goroutine stuck forever.
func (d Delivery) Context() context.Context { return d.ctx }

// Ack explicitly acknowledges this delivery, advancing the subscription's
// committed position if it is the contiguous head.
func (d Delivery) Ack() error { return d.sub.ack(d.Message.Offset) }

// Nack rejects this delivery, scheduling redelivery with Attempt+1.
func (d Delivery) Nack() error { return d.sub.nack(d.Message.Offset) }

// Handler processes one delivery.
//
// Returning nil acknowledges the message at its offset. Returning an error
// makes the broker redeliver it with an incremented Attempt counter. The
// offset is committed only once, contiguously; duplicate deliveries can be
// recognized via Message.ID / Message.Attempt.
type Handler interface {
	Handle(ctx context.Context, d Delivery) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, d Delivery) error

func (f HandlerFunc) Handle(ctx context.Context, d Delivery) error { return f(ctx, d) }

// RejectHandler receives messages that could not be decoded for a subscriber.
// It lets the application choose a distinguishable action (e.g. quarantine
// and d.Ack(), or d.Nack() to retry) instead of having the message silently
// skipped. If no RejectHandler is configured the subscription parks at the
// rejected offset; see Subscription.BlockedOn.
type RejectHandler interface {
	OnReject(ctx context.Context, d Delivery, cause error) error
}

// RejectHandlerFunc adapts a function to RejectHandler.
type RejectHandlerFunc func(ctx context.Context, d Delivery, cause error) error

func (f RejectHandlerFunc) OnReject(ctx context.Context, d Delivery, cause error) error {
	return f(ctx, d, cause)
}

// Subscription is one named or anonymous attachment of a handler to a topic.
type Subscription struct{ sub *subState }

// Ack acknowledges the message at off.
func (s *Subscription) Ack(off Offset) error { return s.sub.ack(off) }

// Nack schedules immediate redelivery of the message at off.
func (s *Subscription) Nack(off Offset) error { return s.sub.nack(off) }

// Detach temporarily stops delivery (a slow/temporarily-offline subscriber).
// In-flight dispatch slots are reclaimed and, on re-attach, every
// uncommitted message is redelivered from commit+1; Attempt > 1 marks these
// messages as redelivery. The durable committed position is never lost.
func (s *Subscription) Detach() error {
	s.sub.mu.Lock()
	if s.sub.closed {
		s.sub.mu.Unlock()
		return ErrClosed
	}
	if !s.sub.attached {
		s.sub.mu.Unlock()
		return ErrNotAttached
	}
	s.sub.attached = false
	s.sub.resetInflightLocked()
	s.sub.kickLocked()
	s.sub.mu.Unlock()
	// An offline subscriber exerts no backpressure: release any publisher
	// blocked on it (the post-wake re-check sees it inactive).
	s.sub.topic.admCond.Broadcast()
	// Wait for the dispatcher goroutine to observe detached state and exit,
	// then cancel and drain any in-flight handler.
	<-s.sub.doneCh
	s.sub.goOffline()
	return nil
}

// Attach re-activates a detached subscription, resuming strictly at commit+1.
func (s *Subscription) Attach(ctx context.Context) error {
	s.sub.topic.mu.Lock()
	s.sub.mu.Lock()
	if s.sub.closed {
		s.sub.mu.Unlock()
		s.sub.topic.mu.Unlock()
		return ErrClosed
	}
	if s.sub.attached {
		s.sub.mu.Unlock()
		s.sub.topic.mu.Unlock()
		return nil
	}
	s.sub.attached = true
	s.sub.goOnline()
	s.sub.resetInflightLocked()
	s.sub.spawnLocked()
	s.sub.kickLocked()
	s.sub.mu.Unlock()
	s.sub.topic.mu.Unlock()
	// Backpressure is re-applied lazily by the next publish's evaluateLocked.
	return nil
}

// CommitPosition returns the highest contiguous offset acknowledged so far.
// Re-attaching a durable subscription resumes at CommitPosition()+1.
func (s *Subscription) CommitPosition() Offset {
	s.sub.mu.Lock()
	defer s.sub.mu.Unlock()
	return s.sub.commit
}

// BlockedOn reports whether delivery is parked on an undecodable message and
// returns its offset and the rejection cause (use AsRejectError to classify
// the RejectReason).
func (s *Subscription) BlockedOn() (Offset, error, bool) { return s.sub.blockedOn() }

// Close detaches and unregisters the subscription. Named subscriptions retain
// their committed cursor in the broker for later re-subscription.
func (s *Subscription) Close() error { return s.sub.broker.closeSub(s.sub) }
