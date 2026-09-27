package pubsub

import "sync"

// topicLog is a per-topic append-only log. Offsets are dense and monotonic;
// trimming only ever removes a prefix that every subscription has acknowledged,
// so resumption never lands on a missing message.
type topicLog struct {
	name string

	mu      sync.Mutex
	base    Offset     // offset of msgs[0]
	msgs    []*Message // messages at offsets base..base+len(msgs)-1
	retain  int        // 0 = unlimited
	subs    map[*subState]struct{}
	cursors map[string]Offset // committed offset per durable subscription name
	nextID  int64

	// admMu serializes publisher admission. Exactly one publisher at a time
	// performs an admit-check; if the slowest subscriber is at capacity that
	// publisher waits on admCond (broadcast by every ack) and then RECHECKS.
	// This makes admission fair and bounded: a not-yet-full fast publisher
	// cannot steal the slot a blocked publisher is waiting for.
	admMu   sync.Mutex
	admCond *sync.Cond
}

func newTopicLog(name string, retain int) *topicLog {
	t := &topicLog{
		name:    name,
		retain:  retain,
		subs:    make(map[*subState]struct{}),
		cursors: make(map[string]Offset),
	}
	t.admCond = sync.NewCond(&t.admMu)
	return t
}

// appendLocked assigns an offset, stores msg and returns that offset.
// Caller holds t.mu.
func (t *topicLog) appendLocked(msg *Message) Offset {
	off := t.base + Offset(len(t.msgs))
	msg.Offset = off
	t.msgs = append(t.msgs, msg)
	t.trimLocked()
	return off
}

// lengthLocked reports the next offset to assign. Caller holds t.mu.
func (t *topicLog) lengthLocked() Offset { return t.base + Offset(len(t.msgs)) }

// lengthEnd reports the exclusive end offset (the next offset to assign).
func (t *topicLog) lengthEnd() Offset {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lengthLocked()
}

// at returns the message at off.
func (t *topicLog) at(off Offset) (*Message, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if off < t.base {
		return nil, false
	}
	i := int(off - t.base)
	if i >= len(t.msgs) {
		return nil, false
	}
	return t.msgs[i], true
}

// trimLocked drops acknowledged prefix messages while keeping at most t.retain
// messages. It never removes a message at or above any subscription's
// committed position. Caller holds t.mu; lock order topic -> sub is allowed.
func (t *topicLog) trimLocked() {
	if t.retain <= 0 {
		return
	}
	// Lowest position that must still be readable: commit+1 for every sub.
	watermark := t.lengthLocked() // exclusive lower bound of trimmable prefix
	for s := range t.subs {
		s.mu.Lock()
		c := s.commit
		s.mu.Unlock()
		if c+1 < watermark {
			watermark = c + 1
		}
	}
	for _, c := range t.cursors {
		if c+1 < watermark {
			watermark = c + 1
		}
	}
	maxTrim := len(t.msgs) - t.retain
	for maxTrim > 0 && t.base < watermark {
		t.msgs = t.msgs[1:]
		t.base++
		maxTrim--
	}
	if len(t.msgs) == 0 {
		// Keep backing array from retaining trimmed payloads.
		t.msgs = nil
	}
}

func (t *topicLog) addSubLocked(s *subState)    { t.subs[s] = struct{}{} }
func (t *topicLog) removeSubLocked(s *subState) { delete(t.subs, s) }

func (t *topicLog) cursorLocked(name string) (Offset, bool) {
	c, ok := t.cursors[name]
	return c, ok
}

func (t *topicLog) setCursorLocked(name string, off Offset) { t.cursors[name] = off }
