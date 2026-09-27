package pubsub

import "errors"

// 可区分的错误类型，调用方可用 errors.Is 判定。
var (
	// ErrBackpressure 订阅方过慢且策略为 FailFast 时，发布被拒绝。
	ErrBackpressure = errors.New("pubsub: backpressure, subscriber too slow")
	// ErrVersionMismatch 消息结构版本不在订阅方支持的范围内。
	ErrVersionMismatch = errors.New("pubsub: schema version mismatch")
	// ErrMissingField 解码后必填字段缺失。
	ErrMissingField = errors.New("pubsub: required field missing")
	// ErrUnknownField 消息中出现订阅方不认识的字段。
	ErrUnknownField = errors.New("pubsub: unknown field")
	// ErrUnknownTopic 主题不存在。
	ErrUnknownTopic = errors.New("pubsub: unknown topic")
	// ErrClosed 组件已关闭。
	ErrClosed = errors.New("pubsub: closed")
)
