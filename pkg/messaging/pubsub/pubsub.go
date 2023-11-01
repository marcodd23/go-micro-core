package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/marcodd23/go-micro-lib/pkg/messaging"
	"google.golang.org/api/option"
	"time"
)

// NewPubSubBufferedPublisherFactory - factory that create a pubsub client and then initialize a BufferedPublisher.
func NewPubSubBufferedPublisherFactory(
	ctx context.Context,
	projectID string,
	batchSize int32,
	flushDelayThreshold *time.Duration,
	opts ...option.ClientOption) (messaging.BufferedPublisher, error) {
	client, err := pubsub.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, messaging.NewMessagingErrorCode(messaging.ErrorInitializingPubsubClient, err)
	}

	publishConfig := messaging.TopicPublishConfig{
		BatchSize: batchSize,
	}

	if flushDelayThreshold == nil {
		publishConfig.FlushDelayThreshold = messaging.DefaultFlushDelayThreshold
	} else {
		publishConfig.FlushDelayThreshold = *flushDelayThreshold
	}

	pubSubClient := &pubSubClient{client: client}

	return messaging.NewBufferedPublisher(pubSubClient, publishConfig)
}

// NewBufferedPublisherWithRetryFactory - factory that create a pubsub client and then initialize a BufferedPublisher.
func NewBufferedPublisherWithRetryFactory(
	ctx context.Context,
	projectID string,
	batchSize int32,
	flushDelayThreshold *time.Duration,
	maxRetryCount int16,
	initialRetryInterval *time.Duration,
	opts ...option.ClientOption) (messaging.BufferedPublisherWithRetry, error) {
	client, err := pubsub.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, messaging.NewMessagingErrorCode(messaging.ErrorInitializingPubsubClient, err)
	}

	publishConfig := messaging.TopicPublishConfig{
		BatchSize: batchSize,
	}

	if flushDelayThreshold == nil {
		publishConfig.FlushDelayThreshold = messaging.DefaultFlushDelayThreshold
	} else {
		publishConfig.FlushDelayThreshold = *flushDelayThreshold
	}

	if initialRetryInterval == nil {
		publishConfig.InitialRetryInterval = messaging.DefaultInitialRetryInterval
	} else {
		publishConfig.InitialRetryInterval = *initialRetryInterval
	}

	pubSubClient := &pubSubClient{client: client}

	return messaging.NewBufferedPublisherWithRetry(ctx, pubSubClient, publishConfig)
}

// pubSubClient - Client implementation for PubSub.
type pubSubClient struct {
	client *pubsub.Client
}

func (w *pubSubClient) Topic(id string) messaging.Topic {
	realTopic := w.client.Topic(id)
	return &pubSubTopic{topic: realTopic}
}

func (w *pubSubClient) Close() error {
	return w.client.Close()
}

// pubSubTopic - Topic implementation for PubSub.
type pubSubTopic struct {
	topic *pubsub.Topic
}

func (w *pubSubTopic) Publish(ctx context.Context, msg messaging.Message) messaging.PublishResult {
	pubSubMessage := &pubsub.Message{
		Attributes: msg.GetAttributes(),
		Data:       msg.GetPayload(),
	}
	return w.topic.Publish(ctx, pubSubMessage)
}

func (w *pubSubTopic) Stop() {
	w.topic.Stop()
}

func (w *pubSubTopic) Flush() {
	w.topic.Flush()
}

func (w *pubSubTopic) String() string {
	return w.topic.String()
}

func (w *pubSubTopic) ConfigPublishSettings(config messaging.TopicPublishConfig) {
	w.topic.PublishSettings.CountThreshold = int(config.BatchSize)
	w.topic.PublishSettings.DelayThreshold = config.FlushDelayThreshold
}
