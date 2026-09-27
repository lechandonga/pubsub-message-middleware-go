package pubsub

import (
	"context"
	"sync"
)

// Subscription 是一个具名订阅。
//
// 语义：
//   - 至少一次投递：只有 Ack 之后位置才推进；
//   - 断线重连（同名再订阅）后从上次确认位置继续，
//     之前已投递未确认的消息会以 Redelivered=true、Attempt>1 重新投递，
//     重复投递因此可被识别；
//   - Ack 幂等：重复 Ack 或 Ack 已确认的消息不会重复记账。
type Subscription struct {
	name  string
	topic string
	b     *Broker

	notify chan struct{}  // 泵信号：有新消息或空间释放
	ch     chan Delivery  // 待投递队列
	done   chan struct{}  // 关闭信号
	once   sync.Once      // 保证 Close 只执行一次
	pumpWg sync.WaitGroup // 等待泵 goroutine 退出

	// 以下字段仅在 broker.mu 保护下访问
	offset   uint64          // 已确认位置：seq <= offset 均已确认
	next     uint64          // 下一条待投递序号
	inflight map[uint64]int  // 已投递未确认：seq -> 已投递次数
	acked    map[uint64]bool // 已 Ack 但尚未连续推进的序号
	closed   bool
}

// Name 返回订阅名。
func (s *Subscription) Name() string { return s.name }

// Topic 返回订阅的主题。
func (s *Subscription) Topic() string { return s.topic }

// Next 取下一条待投递消息。订阅关闭后返回 ErrClosed。
func (s *Subscription) Next(ctx context.Context) (Delivery, error) {
	select {
	case d, ok := <-s.ch:
		if !ok {
			return Delivery{}, ErrClosed
		}
		return d, nil
	case <-s.done:
		// 关闭后仍允许排空已入队的投递
		select {
		case d, ok := <-s.ch:
			if !ok {
				return Delivery{}, ErrClosed
			}
			return d, nil
		default:
			return Delivery{}, ErrClosed
		}
	case <-ctx.Done():
		return Delivery{}, ctx.Err()
	}
}

// Ack 确认一条投递，推进已确认位置。重复确认安全，返回 nil。
func (s *Subscription) Ack(d Delivery) error {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if _, ok := s.inflight[d.Seq]; !ok {
		// 重复 Ack 或未知序号：不重复记账
		return nil
	}
	delete(s.inflight, d.Seq)
	s.acked[d.Seq] = true
	for s.acked[s.offset+1] {
		delete(s.acked, s.offset+1)
		s.offset++
	}
	s.b.signalLocked()
	s.pingLocked()
	return nil
}

// Offset 返回当前已确认位置（seq <= Offset() 的消息均已确认）。
func (s *Subscription) Offset() uint64 {
	s.b.mu.Lock()
	defer s.b.mu.Unlock()
	return s.offset
}

// Close 关闭订阅。已确认位置与未确认消息的投递次数被保留，
// 同名再订阅时据此续传与识别重投。重复 Close 返回 nil。
func (s *Subscription) Close() error {
	s.once.Do(func() {
		s.b.mu.Lock()
		s.closed = true
		s.b.detachLocked(s)
		s.b.mu.Unlock()
		close(s.done)
		s.pumpWg.Wait()
		close(s.ch)
	})
	return nil
}

// pingLocked 非阻塞地通知泵继续工作。调用方须持有 broker.mu。
func (s *Subscription) pingLocked() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}
