package pubsub

import (
	"context"
	publisher2 "github.com/marcodd23/go-micro-core/pkg/patterns/messaging/publisher"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

// NewPubSubBufferedPublisherFactory - factory that create a cloud_pubsub client and then initialize a publisher.BufferedPublisher.
func NewPubSubBufferedPublisherFactory(
	ctx context.Context,
	projectID string,
	batchSize int32,
	flushDelayThreshold int32,
	opts ...option.ClientOption) (publisher2.BufferedPublisher, error) {
	client, err := pubsub.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, publisher2.NewMessagingErrorCode(publisher2.ErrorInitializingPubsubClient, err)
	}

	publishConfig := publisher2.TopicPublishConfig{
		BatchSize: batchSize,
	}

	if flushDelayThreshold <= 0 {
		publishConfig.FlushDelayThreshold = publisher2.DefaultFlushDelayThreshold
	} else {
		publishConfig.FlushDelayThreshold = time.Duration(flushDelayThreshold) * time.Millisecond
	}

	pubSubClient := &pubSubClient{client: client}

	return publisher2.NewBufferedPublisher(pubSubClient, publishConfig)
}

// NewBufferedPublisherWithRetryFactory - factory that create a cloud_pubsub client and then initialize a publisher.BufferedPublisherWithRetry.
func NewBufferedPublisherWithRetryFactory(
	ctx context.Context,
	projectID string,
	batchSize int32,
	flushDelayThresholdMillis int32,
	maxRetryCount int16,
	initialRetryIntervalMillis int32,
	opts ...option.ClientOption) (publisher2.BufferedPublisherWithRetry, error) {
	client, err := pubsub.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, publisher2.NewMessagingErrorCode(publisher2.ErrorInitializingPubsubClient, err)
	}

	publishConfig := publisher2.TopicPublishConfig{}

	if batchSize <= 0 {
		publishConfig.BatchSize = publisher2.DefaultBatchSize
	} else {
		publishConfig.BatchSize = batchSize
	}

	if flushDelayThresholdMillis <= 0 {
		publishConfig.FlushDelayThreshold = publisher2.DefaultFlushDelayThreshold
	} else {
		publishConfig.FlushDelayThreshold = time.Duration(flushDelayThresholdMillis) * time.Millisecond
	}

	if initialRetryIntervalMillis <= 0 {
		publishConfig.InitialRetryInterval = publisher2.DefaultInitialRetryInterval
	} else {
		publishConfig.InitialRetryInterval = time.Duration(initialRetryIntervalMillis) * time.Millisecond
	}

	if maxRetryCount <= 0 {
		publishConfig.MaxRetryCount = publisher2.DefaultMaxRetryCount
	} else {
		publishConfig.MaxRetryCount = maxRetryCount
	}

	pubSubClient := &pubSubClient{client: client}

	return publisher2.NewBufferedPublisherWithRetry(ctx, pubSubClient, publishConfig)
}
