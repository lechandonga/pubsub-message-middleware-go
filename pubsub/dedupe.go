package pubsub

import "sync"

// DedupTracker records message IDs that have already been processed so that
// redelivered messages can be recognised instead of causing double effects.
//
// It is safe for concurrent use. When the tracker reaches capacity the oldest
// tracked ID is evicted; capacity is therefore a window, not permanent memory.
// Typical window sizes are far larger than any redelivery lag.
type DedupTracker struct {
	mu       sync.Mutex
	seen     map[MessageID]struct{}
	order    []MessageID
	capacity int
}

// DefaultDedupCapacity bounds the dedup window.
const DefaultDedupCapacity = 65536

// NewDedupTracker creates an empty tracker with the default capacity.
func NewDedupTracker() *DedupTracker {
	return NewDedupTrackerCapacity(DefaultDedupCapacity)
}

// NewDedupTrackerCapacity creates an empty tracker with the given window size.
func NewDedupTrackerCapacity(capacity int) *DedupTracker {
	if capacity <= 0 {
		capacity = DefaultDedupCapacity
	}
	return &DedupTracker{
		seen:     make(map[MessageID]struct{}, capacity),
		order:    make([]MessageID, 0, capacity),
		capacity: capacity,
	}
}

// Seen reports whether id was already recorded.
func (d *DedupTracker) Seen(id MessageID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.seen[id]
	return ok
}

// Mark records id. It returns true if it was already present (duplicate).
func (d *DedupTracker) Mark(id MessageID) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[id]; ok {
		return true
	}
	d.seen[id] = struct{}{}
	d.order = append(d.order, id)
	if len(d.order) > d.capacity {
		victim := d.order[0]
		d.order = d.order[1:]
		delete(d.seen, victim)
	}
	return false
}

// Len returns the number of IDs currently held in the window.
func (d *DedupTracker) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}
