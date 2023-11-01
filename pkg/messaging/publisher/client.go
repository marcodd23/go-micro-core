package publisher

import (
	"context"
	"time"
)

const (
	DefaultInitialRetryInterval = 50 * time.Millisecond
	DefaultFlushDelayThreshold  = time.Millisecond * 10
)

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

// TopicPublishConfig - configuration struct for the publisher.
type TopicPublishConfig struct {
	BatchSize            int32
	FlushDelayThreshold  time.Duration
	InitialRetryInterval time.Duration
	MaxRetryCount        int16
}
