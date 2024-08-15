package publisher

import (
	"context"
	"time"

	"github.com/marcodd23/go-micro-core/pkg/messaging"
)

const (
	DefaultInitialRetryInterval       = 50 * time.Millisecond
	DefaultFlushDelayThreshold        = time.Millisecond * 10
	DefaultMaxRetryCount        int16 = 3
	DefaultBatchSize            int32 = 1000
)

// ============================================
// Client Interface
// ============================================

// Client -  Client wrapper interface.
type Client interface {
	Topic(id string) Topic
	Close() error
}

// ============================================
// Topic Interface
// ============================================

// Topic - Topic wrapper interface.
type Topic interface {
	Publish(ctx context.Context, msg messaging.Message) PublishResult
	Stop()
	Flush()
	String() string
	ConfigPublishSettings(config TopicPublishConfig)
}

// ============================================
// Publish Result Interface
// ============================================

// PublishResult - PubSub Publish Result wrapper interface.
type PublishResult interface {
	Get(ctx context.Context) (string, error)
	Ready() <-chan struct{}
}

// ============================================
// Publisher Configuration
// ============================================

// TopicPublishConfig - configuration struct for the publisher.
type TopicPublishConfig struct {
	BatchSize            int32
	FlushDelayThreshold  time.Duration
	InitialRetryInterval time.Duration
	MaxRetryCount        int16
}
