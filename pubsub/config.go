package pubsub

// BackpressurePolicy 决定订阅方积压时的行为。
type BackpressurePolicy int

const (
	// Block 发布方阻塞等待，直到订阅方消化出空间。
	Block BackpressurePolicy = iota
	// FailFast 发布方立即收到 ErrBackpressure，不丢数据也不阻塞。
	FailFast
)

// Config 是 Broker 的配置。
type Config struct {
	MaxInFlight        int                // 每个订阅允许的最大未确认消息数
	BackpressurePolicy BackpressurePolicy // 积压策略
	BufferSize         int                // 每个订阅的投递通道缓冲
}

func (c Config) withDefaults() Config {
	if c.MaxInFlight <= 0 {
		c.MaxInFlight = 64
	}
	if c.BufferSize <= 0 {
		c.BufferSize = 64
	}
	return c
}
