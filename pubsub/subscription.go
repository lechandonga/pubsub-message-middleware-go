package pubsub

import (
	"context"
	"sync"
)

// subState is the internal state of one subscription.
//
// Lock ordering (must be strictly observed to avoid deadlocks):
//
//	broker.mu -> topic.mu -> sub.mu
//
// The dispatcher never holds sub.mu while acquiring topic.mu; it snapshots the
// candidate offset under sub.mu, fetches the message with only topic.mu held,
// then re-validates under sub.mu.
type subState struct {
	broker *Broker
	topic  *topicLog

	name        string
	wantVersion SchemaVersion
	handler     Handler
	rejecter    RejectHandler
	bp          BackpressureConfig

	mu       sync.Mutex
	commit   Offset // highest CONTIGUOUS acknowledged offset
	nextDel  Offset // next offset to dispatch
	attached bool
	closed   bool
	inflight map[Offset]struct{} // dispatched, awaiting ack
	acked    map[Offset]struct{} // acked ahead of an unfinished gap
	attempts map[Offset]int      // delivery attempt counter per offset
	rejected map[Offset]error    // parked decode rejections
	cause    error

	stopCh chan struct{} // closed on Close
	wakeCh chan struct{} // 1-buffered dispatcher wake-up
	doneCh chan struct{} // closed while no dispatcher goroutine runs
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup // dispatcher goroutine
	hwg    sync.WaitGroup // in-flight handler/reject goroutines
}

func newSubState(b *Broker, t *topicLog, name string, want SchemaVersion,
	h Handler, rh RejectHandler, bp BackpressureConfig) *subState {
	s := &subState{
		broker:      b,
		topic:       t,
		name:        name,
		wantVersion: want,
		handler:     h,
		rejecter:    rh,
		bp:          bp,
		commit:      -1,
		inflight:    make(map[Offset]struct{}),
		acked:       make(map[Offset]struct{}),
		attempts:    make(map[Offset]int),
		rejected:    make(map[Offset]error),
		stopCh:      make(chan struct{}),
		wakeCh:      make(chan struct{}, 1),
		doneCh:      nil,
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	return s
}

// startLocked launches the dispatcher at start. Caller holds topic.mu.
func (s *subState) startLocked(start Offset) {
	s.mu.Lock()
	s.commit = start - 1
	s.nextDel = start
	s.attached = true
	s.spawnLocked()
	s.mu.Unlock()
}

// spawnLocked starts a dispatcher goroutine if none is running.
// Callers must ensure any previous loop has exited (Detach waits on doneCh).
func (s *subState) spawnLocked() {
	if s.doneCh != nil {
		select {
		case <-s.doneCh:
			// previous loop has fully exited; replace the signal channel
		default:
			return // already running
		}
	}
	s.doneCh = make(chan struct{})
	s.wg.Add(1)
	go s.loop()
}

// loop hands messages to handlers, in offset order, at most MaxInFlight at a
// time. It parks while detached, caught up, saturated, or parked on a decode
// rejection — never busy-waiting and never skipping the head.
func (s *subState) loop() {
	defer func() {
		close(s.doneCh)
		s.wg.Done()
	}()
	for {
		end := s.topic.lengthEnd()
		off, kind := s.plan(end)
		switch kind {
		case loopClosed:
			return
		case mustPark:
			if !s.park(end) {
				return
			}
		case canDispatch:
			s.dispatch(off)
		}
	}
}

type waitKind int

const (
	canDispatch waitKind = iota
	mustPark
	loopClosed
)

// plan decides the next action. end is the topic's exclusive end offset read
// WITHOUT holding sub.mu (lock order: topic.mu -> sub.mu, never reversed).
func (s *subState) plan(end Offset) (Offset, waitKind) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// A detached or closed subscription means the dispatcher goroutine must
	// exit; Attach (or resubscribe) starts a fresh one after doneCh closes.
	if s.closed || !s.attached {
		return 0, loopClosed
	}
	if len(s.inflight) >= s.bp.MaxInFlight {
		return 0, mustPark
	}
	if s.nextDel >= end {
		return 0, mustPark
	}
	if cause, parked := s.rejected[s.nextDel]; parked {
		s.cause = cause
		return 0, mustPark
	}
	return s.nextDel, canDispatch
}

// dispatch delivers off, re-validating state after the (lock-free) topic read.
func (s *subState) dispatch(off Offset) {
	msg, ok := s.topic.at(off)
	if !ok {
		// Trimmed before we could read it; re-plan.
		s.kick()
		return
	}
	s.mu.Lock()
	if s.closed || !s.attached {
		s.mu.Unlock()
		return
	}
	if off != s.nextDel {
		s.mu.Unlock() // state moved on; re-plan
		return
	}
	if _, busy := s.inflight[off]; busy {
		s.nextDel = off + 1
		s.mu.Unlock()
		s.kick()
		return
	}
	// Defensive re-check of the slow-subscriber ceiling.
	if int(off-s.commit) > s.bp.PendingLimit {
		s.mu.Unlock()
		return
	}
	s.attempts[off]++
	n := s.attempts[off]
	s.inflight[off] = struct{}{}
	s.nextDel = off + 1
	s.mu.Unlock()

	if n > 1 {
		s.broker.logger.Redelivered(s.topic.name, msg.ID, off, n)
	} else {
		s.broker.logger.Delivered(s.topic.name, msg.ID, off, n)
	}
	s.deliverOne(msg, n)
	s.kick() // try to fill remaining slots
}

// park blocks until kicked, detached or closed. Returns false when the loop
// must exit.
func (s *subState) park(end Offset) bool {
	s.mu.Lock()
	stop := s.stopCh
	wake := s.wakeCh
	if s.closed || !s.attached {
		s.mu.Unlock()
		return false
	}
	// Drain any stale kick token before sleeping, so the NEXT kick is
	// guaranteed to wake us (1-buffered channel discipline).
	select {
	case <-wake:
	default:
	}
	// Re-check conditions after draining: the kick that carried the stale
	// token may already have made work available.
	hasWork := s.nextDel < end &&
		len(s.inflight) < s.bp.MaxInFlight && s.rejected[s.nextDel] == nil
	s.mu.Unlock()
	if hasWork {
		return true
	}
	select {
	case <-stop:
		return false
	case <-wake:
		return true
	}
}

// deliverOne decodes and invokes the handler. It runs in its own goroutine so
// that MaxInFlight>1 genuinely allows concurrent handlers (with MaxInFlight=1
// there is at most one). Decode failures are never skipped: they are routed to
// the RejectHandler or park the subscription.
func (s *subState) deliverOne(msg *Message, attemptNo int) {
	s.hwg.Add(1)
	go func() {
		defer s.hwg.Done()
		payload, derr := s.broker.reg.Decode(s.topic.name, s.wantVersion, msg.Data, msg.SchemaVersion)
		d := Delivery{Message: cloneForDelivery(msg, attemptNo), Payload: payload, sub: s, ctx: s.ctx}
		if derr != nil {
			s.parkOnReject(msg.Offset, derr)
			return
		}
		if err := s.handler.Handle(s.ctx, d); err != nil {
			// A handler failure cannot lose the message: it stays uncommitted
			// and is redelivered with a higher Attempt.
			_ = s.nack(msg.Offset)
			return
		}
		_ = s.ack(msg.Offset)
	}()
}

// parkOnReject records the rejection. With a RejectHandler it is invoked for
// an explicit decision; otherwise the subscription deterministically parks at
// this offset (inspectable via BlockedOn) until Ack releases it.
func (s *subState) parkOnReject(off Offset, cause error) {
	s.mu.Lock()
	delete(s.inflight, off)
	s.rejected[off] = cause
	s.cause = cause
	attNo := s.attempts[off]
	s.mu.Unlock()

	re, _ := AsRejectError(cause)
	var reason RejectReason = "unknown"
	var id MessageID
	if re != nil {
		reason = re.Reason
		id = re.ID
	}
	if msg, ok := s.topic.at(off); ok {
		id = msg.ID
	}
	s.broker.logger.Rejected(s.topic.name, id, off, reason)

	if s.rejecter != nil {
		// Hand the application the exact, distinguishable reason. It is the
		// application's choice to Ack (quarantine) or Nack (retry).
		msg, ok := s.topic.at(off)
		if !ok {
			msg = &Message{Topic: s.topic.name, Offset: off}
		}
		d := Delivery{Message: cloneForDelivery(msg, attNo), sub: s, ctx: s.ctx}
		_ = s.rejecter.OnReject(s.ctx, d, cause)
	}
}

func cloneForDelivery(m *Message, n int) *Message {
	cp := *m
	cp.Attempt = n
	if m.Headers != nil {
		cp.Headers = make(map[string]string, len(m.Headers))
		for k, v := range m.Headers {
			cp.Headers[k] = v
		}
	}
	if m.Data != nil {
		cp.Data = append([]byte(nil), m.Data...)
	}
	return &cp
}

func (s *subState) ack(off Offset) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	_, inflight := s.inflight[off]
	_, wasRejected := s.rejected[off]
	if off <= s.commit {
		s.mu.Unlock()
		return ErrAlreadyAcked
	}
	if !inflight && !wasRejected {
		s.mu.Unlock()
		return ErrBadAck
	}
	delete(s.inflight, off)
	delete(s.rejected, off)
	s.acked[off] = struct{}{}
	// Collapse the contiguous acked prefix.
	for {
		next := s.commit + 1
		if _, ok := s.acked[next]; !ok {
			break
		}
		delete(s.acked, next)
		delete(s.attempts, next)
		s.commit = next
	}
	commit := s.commit
	s.cause = nil
	s.kickLocked()
	s.mu.Unlock()

	if s.name != "" {
		// Persist the durable cursor without holding sub.mu (lock order).
		s.topic.mu.Lock()
		s.topic.setCursorLocked(s.name, commit)
		s.topic.mu.Unlock()
	}
	s.broker.logger.Acked(s.topic.name, s.name, off, commit)
	// Every contiguous ack may free a publish slot; wake waiting publishers.
	s.topic.admCond.Broadcast()
	return nil
}

// nack schedules redelivery of off without moving the committed position.
// Redelivery carries a strictly higher Attempt number.
func (s *subState) nack(off Offset) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if off <= s.commit {
		return ErrAlreadyAcked
	}
	if _, ok := s.inflight[off]; !ok {
		if _, parked := s.rejected[off]; parked {
			// Retry a parked rejection: clear the park and re-dispatch.
			delete(s.rejected, off)
			if off < s.nextDel {
				s.nextDel = off
			}
			s.kickLocked()
			return nil
		}
		return ErrBadAck
	}
	delete(s.inflight, off)
	if off < s.nextDel {
		s.nextDel = off
	}
	s.kickLocked()
	return nil
}

// resetInflightLocked rewinds delivery to commit+1, discarding dispatch
// slots. Attempt counters survive, so rewound messages are visibly
// redelivered (Attempt > 1). Caller holds s.mu.
func (s *subState) resetInflightLocked() {
	s.inflight = make(map[Offset]struct{})
	// Keep acked-ahead entries: they were durably processed.
	s.nextDel = s.commit + 1
	for off := range s.acked {
		if off < s.nextDel {
			s.nextDel = off + 1
		}
	}
	// Repark any rejected heads that are still ahead.
	for off, cause := range s.rejected {
		if off < s.nextDel {
			s.nextDel = off
			s.cause = cause
		}
	}
}

func (s *subState) kick() {
	s.mu.Lock()
	s.kickLocked()
	s.mu.Unlock()
}

// goOffline cancels in-flight handler contexts and waits for them to finish.
// Detach represents a subscriber going offline, so handlers must not keep
// running against a subscription that is about to redeliver from commit+1.
func (s *subState) goOffline() {
	s.cancel()
	s.hwg.Wait()
}

// goOnline installs a fresh handler context for the next attached epoch.
func (s *subState) goOnline() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *subState) kickLocked() {
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

// blockedOn reports the offset and cause when delivery is parked on a message
// it could not decode.
func (s *subState) blockedOn() (Offset, error, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for off, cause := range s.rejected {
		return off, cause, true
	}
	return 0, nil, false
}
