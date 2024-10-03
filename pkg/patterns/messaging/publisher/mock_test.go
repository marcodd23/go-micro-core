//nolint:all
package publisher_test

import (
	"context"
	"github.com/marcodd23/go-micro-core/pkg/patterns/messaging"
	"github.com/marcodd23/go-micro-core/pkg/patterns/messaging/publisher"

	"cloud.google.com/go/pubsub"
)

// MockClient - mock a messaging.Client
type MockClient struct {
	topics map[string]publisher.Topic
}

func (c *MockClient) Topic(id string) publisher.Topic {
	return c.topics[id]
}

func (c *MockClient) Close() error {
	return nil
}

// MockTopic - mock a messaging.Topic
type MockTopic struct {
	topic                     *pubsub.Topic
	publishFunc               func(ctx context.Context, msg messaging.Message) publisher.PublishResult
	stopFunc                  func()
	flushFunc                 func()
	stringFunc                func() string
	id                        string
	configPublishSettingsFunc func(config func(topic *pubsub.Topic))
}

func (t MockTopic) Publish(ctx context.Context, msg messaging.Message) publisher.PublishResult {
	return t.publishFunc(ctx, msg)
}

func (t MockTopic) Stop() {
	//t.stopFunc()
}

func (t MockTopic) Flush() {
	// t.flushFunc()
}

func (t MockTopic) String() string {
	return t.id
}

func (t MockTopic) ConfigPublishSettings(config publisher.TopicPublishConfig) {
	// t.configPublishSettingsFunc(config)
}

// MockPublishResult - mock a messaging.PublishResult
type MockPublishResult struct {
	getFunc func(ctx context.Context) (string, error)
	readyCh chan struct{}
}

func (pr MockPublishResult) Get(ctx context.Context) (string, error) {
	return pr.getFunc(ctx)
}

func (pr MockPublishResult) Ready() <-chan struct{} {
	return pr.readyCh
}
