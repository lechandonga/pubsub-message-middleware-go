// Package pubsub is a small, in-process publish/subscribe middleware built
// around per-topic, dense monotonic offset logs.
//
// It is designed so that delivery outcomes stay consistent, reproducible and
// explainable when subscribers are slow, briefly offline (Detach/Attach or
// durable re-subscription), or pinned to an incompatible schema version:
//
//   - Delivery is at-least-once with ordered, contiguous commits. A message is
//     redelivered (Message.Attempt incremented, Message.ID stable) until the
//     contiguous head is acknowledged; gaps can never be skipped. Use
//     DedupTracker with Message.ID to make processing idempotent.
//   - Named subscriptions (WithSubscriptionName) resume at commit+1 across
//     Detach/Attach or re-subscription within one Broker; messages published
//     while offline accumulate in the log and are delivered on re-attach.
//   - Schema compatibility is explicit via Registry codecs and Migrators.
//     Missing required fields, unknown fields, missing/unknown versions and
//     incompatible versions produce distinct RejectError reasons instead of
//     being silently ignored. Undecodable messages park the subscription (or
//     are routed to a RejectHandler) rather than being skipped.
//   - Backpressure is deterministic: PolicyBlock (default) makes over-limit
//     publishes wait without dropping data; PolicyReject returns
//     ErrBackpressure before the message enters the log.
//
// See the Broker, Registry and BackpressureConfig types and docs/design.md
// for the full contract and configuration reference.
package pubsub
