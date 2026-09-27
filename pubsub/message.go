package pubsub

import "time"

// Message 是发布到主题的一条消息。
type Message struct {
	ID          string            // 发布方给定的唯一标识，用于去重与日志
	Topic       string            // 目标主题
	Seq         uint64            // 主题内单调递增序号，由 Broker 分配
	Version     uint32            // 消息结构版本
	Headers     map[string]string // 可选元数据
	Payload     []byte            // 已序列化的消息体
	PublishedAt time.Time
}

// Delivery 是投递给订阅方的一份消息，附带投递元信息。
type Delivery struct {
	Message
	Attempt     int  // 第几次投递，从 1 开始；>1 表示重投
	Redelivered bool // 是否为重复投递（断线重连或重试导致）
}
