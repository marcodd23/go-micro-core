//nolint:all
package messaging

import (
	"cloud.google.com/go/pubsub"
	"context"
	"time"
)

const (
	DefaultInitialRetryInterval = 50 * time.Millisecond
	DefaultFlushDelayThreshold  = time.Millisecond * 10
)

// Message - pubsub message payload interface
type Message interface {
	GetMsgRefId() string
	GetPayload() []byte
	GetAttributes() map[string]string
}

// Client -  Client wrapper interface.
type Client interface {
	Topic(id string) Topic
	Close() error
}

// Topic - Topic wrapper interface.
type Topic interface {
	Publish(ctx context.Context, msg Message) PublishResult
	Stop()
	Flush()
	String() string
	ConfigPublishSettings(config TopicPublishConfig)
}

// PublishResult - PubSub Publish Result wrapper interface.
type PublishResult interface {
	Get(ctx context.Context) (string, error)
	Ready() <-chan struct{}
}

// IMPLEMENTATIONS

// MsgPayload - MsgPayload Payload model implementing Message interface.
type MsgPayload struct {
	// MessageId - Pubsub message id
	MessageId string
	// Data - Pubsub message payload
	Data []byte
	// Attributes - Pubsub topic attributes
	Attributes map[string]string
}

// GetMsgRefId - Get pubsub message id
func (msg *MsgPayload) GetMsgRefId() string {
	return msg.MessageId
}

// GetPayload - Get pubsub message payload
func (msg *MsgPayload) GetPayload() []byte {
	return msg.Data
}

// GetAttributes - Get pubsub attributes
func (msg *MsgPayload) GetAttributes() map[string]string {
	return msg.Attributes
}

// PubSubPublishResult - implementation of PublishResult.
type PubSubPublishResult struct {
	publishResult *pubsub.PublishResult
}

func (prw *PubSubPublishResult) Get(ctx context.Context) (string, error) {
	return prw.publishResult.Get(ctx)
}

func (prw *PubSubPublishResult) Ready() <-chan struct{} {
	return prw.publishResult.Ready()
}

// TopicPublishConfig - configuration struct for the publisher.
type TopicPublishConfig struct {
	BatchSize            int32
	FlushDelayThreshold  time.Duration
	InitialRetryInterval time.Duration
	MaxRetryCount        int16
}
