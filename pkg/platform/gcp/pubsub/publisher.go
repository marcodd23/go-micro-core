package pubsub

import (
	"context"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/marcodd23/go-micro-core/pkg/messaging/publisher"
	"google.golang.org/api/option"
)

// NewPubSubBufferedPublisherFactory - factory that create a cloud_pubsub client and then initialize a publisher.BufferedPublisher.
func NewPubSubBufferedPublisherFactory(
	ctx context.Context,
	projectID string,
	batchSize int32,
	flushDelayThreshold int32,
	opts ...option.ClientOption) (publisher.BufferedPublisher, error) {
	client, err := pubsub.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, publisher.NewMessagingErrorCode(publisher.ErrorInitializingPubsubClient, err)
	}

	publishConfig := publisher.TopicPublishConfig{
		BatchSize: batchSize,
	}

	if flushDelayThreshold <= 0 {
		publishConfig.FlushDelayThreshold = publisher.DefaultFlushDelayThreshold
	} else {
		publishConfig.FlushDelayThreshold = time.Duration(flushDelayThreshold) * time.Millisecond
	}

	pubSubClient := &pubSubClient{client: client}

	return publisher.NewBufferedPublisher(pubSubClient, publishConfig)
}

// NewBufferedPublisherWithRetryFactory - factory that create a cloud_pubsub client and then initialize a publisher.BufferedPublisherWithRetry.
func NewBufferedPublisherWithRetryFactory(
	ctx context.Context,
	projectID string,
	batchSize int32,
	flushDelayThresholdMillis int32,
	maxRetryCount int16,
	initialRetryIntervalMillis int32,
	opts ...option.ClientOption) (publisher.BufferedPublisherWithRetry, error) {
	client, err := pubsub.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, publisher.NewMessagingErrorCode(publisher.ErrorInitializingPubsubClient, err)
	}

	publishConfig := publisher.TopicPublishConfig{}

	if batchSize <= 0 {
		publishConfig.BatchSize = publisher.DefaultBatchSize
	} else {
		publishConfig.BatchSize = batchSize
	}

	if flushDelayThresholdMillis <= 0 {
		publishConfig.FlushDelayThreshold = publisher.DefaultFlushDelayThreshold
	} else {
		publishConfig.FlushDelayThreshold = time.Duration(flushDelayThresholdMillis) * time.Millisecond
	}

	if initialRetryIntervalMillis <= 0 {
		publishConfig.InitialRetryInterval = publisher.DefaultInitialRetryInterval
	} else {
		publishConfig.InitialRetryInterval = time.Duration(initialRetryIntervalMillis) * time.Millisecond
	}

	if maxRetryCount <= 0 {
		publishConfig.MaxRetryCount = publisher.DefaultMaxRetryCount
	} else {
		publishConfig.MaxRetryCount = maxRetryCount
	}

	pubSubClient := &pubSubClient{client: client}

	return publisher.NewBufferedPublisherWithRetry(ctx, pubSubClient, publishConfig)
}
