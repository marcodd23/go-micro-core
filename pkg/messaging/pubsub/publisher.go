package google_pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/marcodd23/go-micro-lib/pkg/messaging"
	"google.golang.org/api/option"
	"time"
)

// NewPubSubBufferedPublisherFactory - factory that create a pubsub client and then initialize a messaging.BufferedPublisher.
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

// NewBufferedPublisherWithRetryFactory - factory that create a pubsub client and then initialize a messaging.BufferedPublisherWithRetry.
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
