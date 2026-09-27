package pubsub

// Offset is a per-topic, monotonically increasing position of a message.
// The first message in a topic is published at offset 0.
type Offset int64

// MessageID is a producer-assigned unique identifier of a message within a topic.
// It is used to detect redelivery and producer retries.
type MessageID string

// SchemaVersion identifies the structural version of a message payload.
type SchemaVersion int

// Message is the immutable envelope stored in a topic log.
type Message struct {
	Topic         string
	ID            MessageID
	Offset        Offset
	SchemaVersion SchemaVersion
	Headers       map[string]string
	// Data is the encoded payload produced by the Codec.
	Data []byte
	// Attempt is the delivery attempt number for the current subscriber: 1 on
	// first delivery, incremented on every redelivery.
	Attempt int
}
