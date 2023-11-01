//nolint:unused
package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/marcodd23/go-micro/pkg/messaging/publisher"
)

// pubSubClient - publisher.Client implementation for PubSub.
type pubSubClient struct {
	client *pubsub.Client
}

func (w *pubSubClient) Topic(id string) publisher.Topic {
	realTopic := w.client.Topic(id)
	return &pubSubTopic{topic: realTopic}
}

func (w *pubSubClient) Close() error {
	return w.client.Close()
}

// pubSubTopic - publisher.Topic implementation for PubSub.
type pubSubTopic struct {
	topic *pubsub.Topic
}

func (w *pubSubTopic) Publish(ctx context.Context, msg publisher.Message) publisher.PublishResult {
	pubSubMessage := &pubsub.Message{
		Attributes: msg.GetAttributes(),
		Data:       msg.GetPayload(),
	}
	return pubSubPublishResult{publishResult: w.topic.Publish(ctx, pubSubMessage)}
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

func (w *pubSubTopic) ConfigPublishSettings(config publisher.TopicPublishConfig) {
	w.topic.PublishSettings.CountThreshold = int(config.BatchSize)
	w.topic.PublishSettings.DelayThreshold = config.FlushDelayThreshold
}

// pubSubPublishResult - publisher.PublishResult implementation for PubSub.
type pubSubPublishResult struct {
	publishResult *pubsub.PublishResult
}

func (prw pubSubPublishResult) Get(ctx context.Context) (string, error) {
	return prw.publishResult.Get(ctx)
}

func (prw pubSubPublishResult) Ready() <-chan struct{} {
	return prw.publishResult.Ready()
}
