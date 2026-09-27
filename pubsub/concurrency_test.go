package pubsub

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// 并发发布订阅：多个发布方与订阅方同时操作，
// 验证无竞态（配合 -race）、无丢失、无重复投递、位置记账正确。
func TestConcurrentPubSub(t *testing.T) {
	const (
		publishers   = 4
		msgsPerPub   = 25
		subscribers  = 3
		total        = publishers * msgsPerPub
		auditSubName = "audit" // 该订阅逐条打印日志
	)
	b := NewBroker(Config{MaxInFlight: 8, BufferSize: 8, BackpressurePolicy: Block})
	defer b.Close()
	ctx := context.Background()

	var wg sync.WaitGroup
	// 每个订阅收到的序号集合，用于检测重复与丢失
	received := make([]map[uint64]int, subscribers)
	for i := range received {
		received[i] = make(map[uint64]int)
	}
	var mu sync.Mutex

	for i := 0; i < subscribers; i++ {
		name := fmt.Sprintf("sub-%d", i)
		if i == 0 {
			name = auditSubName
		}
		sub, err := b.Subscribe(name, "events")
		if err != nil {
			t.Fatalf("Subscribe %s: %v", name, err)
		}
		wg.Add(1)
		go func(idx int, s *Subscription) {
			defer wg.Done()
			for {
				d, err := s.Next(ctx)
				if err != nil {
					t.Errorf("%s Next: %v", s.Name(), err)
					return
				}
				if err := s.Ack(d); err != nil {
					t.Errorf("%s Ack: %v", s.Name(), err)
					return
				}
				mu.Lock()
				received[idx][d.Seq]++
				n := len(received[idx])
				mu.Unlock()
				if s.Name() == auditSubName {
					t.Logf("%s acked id=%s seq=%d offset=%d", s.Name(), d.ID, d.Seq, s.Offset())
				}
				if n == total {
					return
				}
			}
		}(i, sub)
	}

	// 多个发布方并发发布
	var pubWg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		pubWg.Add(1)
		go func(p int) {
			defer pubWg.Done()
			for m := 1; m <= msgsPerPub; m++ {
				id := fmt.Sprintf("pub-%d/msg-%d", p, m)
				if _, err := b.Publish(ctx, "events", Message{ID: id}); err != nil {
					t.Errorf("Publish %s: %v", id, err)
					return
				}
			}
		}(p)
	}
	pubWg.Wait()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("subscribers did not drain all messages in time")
	}

	// 每个订阅必须恰好收到全部消息各一次
	for i, got := range received {
		if len(got) != total {
			t.Fatalf("sub-%d received %d distinct seqs, want %d", i, len(got), total)
		}
		for seq, cnt := range got {
			if cnt != 1 {
				t.Fatalf("sub-%d got seq=%d %d times, want exactly 1", i, seq, cnt)
			}
		}
	}
	t.Logf("all %d subscribers received %d messages each, exactly once", subscribers, total)
}

// 并发重连：订阅方边消费边断开重连，最终位置仍应完整推进。
func TestConcurrentReconnect(t *testing.T) {
	const total = 50
	b := NewBroker(Config{MaxInFlight: 4, BufferSize: 4, BackpressurePolicy: Block})
	defer b.Close()
	ctx := context.Background()

	// 后台持续发布
	pubDone := make(chan struct{})
	go func() {
		defer close(pubDone)
		for i := 1; i <= total; i++ {
			if _, err := b.Publish(ctx, "stream", Message{ID: fmt.Sprintf("stream/msg-%d", i)}); err != nil {
				t.Errorf("Publish: %v", err)
				return
			}
		}
	}()

	// 消费方反复断开重连，用去重集合识别重复投递
	seen := make(map[uint64]int)
	acked := uint64(0)
	for acked < total {
		sub, err := b.Subscribe("flaky", "stream")
		if err != nil {
			t.Fatalf("re-Subscribe: %v", err)
		}
		// 每轮只消费几条就模拟掉线
		for j := 0; j < 5 && acked < total; j++ {
			d, err := sub.Next(ctx)
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			seen[d.Seq]++
			if d.Redelivered {
				t.Logf("redelivered id=%s seq=%d attempt=%d (seen %d times)", d.ID, d.Seq, d.Attempt, seen[d.Seq])
			}
			if err := sub.Ack(d); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			acked = sub.Offset()
		}
		sub.Close()
	}
	<-pubDone
	if acked != total {
		t.Fatalf("final offset = %d, want %d", acked, total)
	}
	for seq := uint64(1); seq <= total; seq++ {
		if seen[seq] == 0 {
			t.Fatalf("seq=%d never delivered", seq)
		}
	}
	t.Logf("resumed across reconnects: offset=%d, all %d messages delivered", acked, total)
}
