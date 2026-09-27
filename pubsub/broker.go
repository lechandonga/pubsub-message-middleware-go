package pubsub

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// topic 是一个主题的追加式日志与其活跃订阅。
type topic struct {
	msgs []Message                  // msgs[i] 的序号为 i+1
	subs map[*Subscription]struct{} // 活跃订阅（断线的不在内）
}

// subState 是订阅的持久状态，断线后保留，用于重连续传。
type subState struct {
	offset  uint64         // 已确认位置
	pending map[uint64]int // 已投递未确认：seq -> 已投递次数
}

// Broker 是本地的发布订阅中枢。
//
// 并发安全：所有共享状态由单一把 mu 保护；
// 订阅位置推进与背压判定都在同一把锁内完成，不会竞态或重复记账。
type Broker struct {
	cfg Config

	mu      sync.Mutex
	changed chan struct{} // 容量 1：任意 Ack/关闭后通知阻塞中的发布方
	closeCh chan struct{} // Close 时关闭，唤醒全部阻塞中的发布方
	topics  map[string]*topic
	states  map[string]*subState     // key: name+"|"+topic，断线后仍保留
	active  map[string]*Subscription // key 同上，当前活跃订阅
	closed  bool
}

// NewBroker 创建一个 Broker。
func NewBroker(cfg Config) *Broker {
	return &Broker{
		cfg:     cfg.withDefaults(),
		changed: make(chan struct{}, 1),
		closeCh: make(chan struct{}),
		topics:  make(map[string]*topic),
		states:  make(map[string]*subState),
		active:  make(map[string]*Subscription),
	}
}

func subKey(name, topic string) string { return name + "|" + topic }

// Publish 向主题追加一条消息，返回分配的序号（从 1 开始单调递增）。
//
// 背压：若某活跃订阅的积压（已追加未确认）超过 MaxInFlight+BufferSize，
//   - FailFast：不追加，返回 ErrBackpressure（含订阅名），不会有部分写入；
//   - Block：阻塞等待该订阅消化，或 ctx 取消。
//
// 已断线的订阅不施加背压，重连后从日志续传。
func (b *Broker) Publish(ctx context.Context, topicName string, msg Message) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, ErrClosed
	}
	t := b.topics[topicName]
	if t == nil {
		t = &topic{subs: make(map[*Subscription]struct{})}
		b.topics[topicName] = t
	}
	for {
		if slow := b.overloadedLocked(t); slow != "" {
			if b.cfg.BackpressurePolicy == FailFast {
				return 0, fmt.Errorf("%w: topic=%q subscriber=%q",
					ErrBackpressure, topicName, slow)
			}
			// Block：等待状态变化、Broker 关闭或 ctx 取消
			b.mu.Unlock()
			select {
			case <-b.changed:
			case <-b.closeCh:
			case <-ctx.Done():
				b.mu.Lock()
				return 0, ctx.Err()
			}
			b.mu.Lock()
			if b.closed {
				return 0, ErrClosed
			}
			continue
		}
		break
	}
	seq := uint64(len(t.msgs)) + 1
	msg.Topic = topicName
	msg.Seq = seq
	msg.PublishedAt = time.Now()
	if msg.ID == "" {
		msg.ID = fmt.Sprintf("%s-%d", topicName, seq)
	}
	t.msgs = append(t.msgs, msg)
	for s := range t.subs {
		s.pingLocked()
	}
	return seq, nil
}

// overloadedLocked 返回第一个积压超限的活跃订阅名，无则返回空串。
func (b *Broker) overloadedLocked(t *topic) string {
	limit := uint64(b.cfg.MaxInFlight + b.cfg.BufferSize)
	for s := range t.subs {
		if uint64(len(t.msgs))-s.offset >= limit {
			return s.name
		}
	}
	return ""
}

// Subscribe 以指定名字订阅主题。
//
// 同名订阅若之前存在（已断线），从上次确认位置继续投递，
// 已投递未确认的消息以 Redelivered=true 重投。
// 同名订阅仍活跃时返回错误，避免同一名字被两处消费导致记账混乱。
func (b *Broker) Subscribe(name, topicName string) (*Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	key := subKey(name, topicName)
	if _, ok := b.active[key]; ok {
		return nil, fmt.Errorf("pubsub: subscription %q on topic %q already active", name, topicName)
	}
	t := b.topics[topicName]
	if t == nil {
		t = &topic{subs: make(map[*Subscription]struct{})}
		b.topics[topicName] = t
	}
	st := b.states[key]
	if st == nil {
		st = &subState{pending: make(map[uint64]int)}
		b.states[key] = st
	}
	s := &Subscription{
		name:     name,
		topic:    topicName,
		b:        b,
		notify:   make(chan struct{}, 1),
		ch:       make(chan Delivery, b.cfg.BufferSize),
		done:     make(chan struct{}),
		offset:   st.offset,
		next:     st.offset + 1,
		inflight: make(map[uint64]int),
		acked:    make(map[uint64]bool),
	}
	// 断线前未确认的消息恢复为待重投，投递次数保留
	for seq, attempts := range st.pending {
		s.inflight[seq] = attempts
	}
	st.pending = make(map[uint64]int)
	t.subs[s] = struct{}{}
	b.active[key] = s
	s.pumpWg.Add(1)
	go s.pump()
	s.pingLocked()
	return s, nil
}

// pump 是订阅的投递泵：在容量允许时把日志中的消息推入投递队列。
func (s *Subscription) pump() {
	defer s.pumpWg.Done()
	for {
		select {
		case <-s.done:
			return
		case <-s.notify:
		}
		for {
			d, ok := s.b.dequeueFor(s)
			if !ok {
				break
			}
			select {
			case s.ch <- d:
			case <-s.done:
				return
			}
		}
	}
}

// dequeueFor 为订阅构造下一条投递；无新消息或达到在途上限时返回 false。
func (b *Broker) dequeueFor(s *Subscription) (Delivery, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s.closed {
		return Delivery{}, false
	}
	if len(s.inflight) >= b.cfg.MaxInFlight {
		return Delivery{}, false
	}
	t := b.topics[s.topic]
	if t == nil || s.next > uint64(len(t.msgs)) {
		return Delivery{}, false
	}
	msg := t.msgs[s.next-1]
	attempts := s.inflight[s.next] // 恢复的重投次数，通常为 0
	s.inflight[s.next] = attempts + 1
	s.next++
	return Delivery{
		Message:     msg,
		Attempt:     attempts + 1,
		Redelivered: attempts > 0,
	}, true
}

// detachLocked 把订阅从活跃集合移除，状态落盘到 states。调用方须持有 mu。
func (b *Broker) detachLocked(s *Subscription) {
	key := subKey(s.name, s.topic)
	delete(b.active, key)
	if t := b.topics[s.topic]; t != nil {
		delete(t.subs, s)
	}
	st := b.states[key]
	st.offset = s.offset
	st.pending = make(map[uint64]int, len(s.inflight))
	for seq, attempts := range s.inflight {
		st.pending[seq] = attempts
	}
	b.signalLocked()
}

// signalLocked 非阻塞地通知阻塞中的发布方状态已变化。调用方须持有 mu。
func (b *Broker) signalLocked() {
	select {
	case b.changed <- struct{}{}:
	default:
	}
}

// Close 关闭 Broker 及全部活跃订阅；持久状态保留在内存中供后续观察。
func (b *Broker) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	close(b.closeCh)
	subs := make([]*Subscription, 0, len(b.active))
	for _, s := range b.active {
		subs = append(subs, s)
	}
	b.mu.Unlock()
	for _, s := range subs {
		s.Close()
	}
	b.mu.Lock()
	b.signalLocked()
	b.mu.Unlock()
}
