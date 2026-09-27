package pubsub

// BrokerConfig configures a Broker.
type BrokerConfig struct {
	// AutoCreateTopics makes Subscribe/Publish create missing topics.
	AutoCreateTopics bool
	// Retention limits how many messages each topic log keeps. 0 = unlimited.
	// Messages are trimmed only after every attached/durable subscription has
	// advanced past them, so retention never causes silent resumption gaps.
	Retention int
	// Backpressure is the default per-subscriber backpressure configuration.
	Backpressure BackpressureConfig
	// Logger, if nil, emits nothing. Use NewStdLogger for line-oriented logs
	// containing message IDs, offsets and commit positions.
	Logger Logger
}

// DefaultBrokerConfig returns a usable default configuration: topics are
// auto-created, retention is unlimited, slow subscribers apply blocking
// backpressure with one in-flight message.
func DefaultBrokerConfig() BrokerConfig {
	return BrokerConfig{
		AutoCreateTopics: true,
		Retention:        0,
		Backpressure:     DefaultBackpressureConfig(),
	}
}
