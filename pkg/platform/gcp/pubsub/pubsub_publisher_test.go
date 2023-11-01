package pubsub_test

import (
	"context"
	"github.com/marcodd23/go-micro-lib/pkg/messaging/publisher"
	"github.com/marcodd23/go-micro-lib/pkg/platform/gcp/pubsub"
	"github.com/marcodd23/go-micro-lib/test/testcontainer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPubSubBufferedPublisher_With_Retry_Proto(t *testing.T) {
	ctx := context.Background()
	container, err := testcontainer.StartPubSubContainer(ctx, "test-project")
	if err != nil {
		t.Fatal(err)
	}
	// Clean up the container after the test is complete
	t.Cleanup(func() {
		if err := container.StopContainer(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

	_ = container.CreateTopic(ctx, t, "test-topic")
	container.CloseClient(ctx, t)
	// subscription := container.CreateSubscription(ctx, t, "test-topic", "test-subscription")
	connOptions := container.CreateConnectionOptions(t)

	// Set up the buffered publisher.
	bp, err := pubsub.NewBufferedPublisherWithRetryFactory(ctx, "test-project", 1, nil, 3, nil, connOptions...)
	require.NoError(t, err)
	// defer bp.Close()

	// Test publishing a single message.
	err = bp.Publish(ctx, "test-topic", &publisher.MsgPayload{Data: []byte("TestMessage1")})
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	// Test flushing the messages.
	err = bp.Flush(ctx)
	assert.NoError(t, err)

	//Test closing the publisher.
	err = bp.Close(ctx)
	assert.NoError(t, err)
}

func TestPubSubBufferedPublisher_With_Retry_Json(t *testing.T) {
	ctx := context.Background()
	container, err := testcontainer.StartPubSubContainer(ctx, "test-project")
	if err != nil {
		t.Fatal(err)
	}
	// Clean up the container after the test is complete
	t.Cleanup(func() {
		if err := container.StopContainer(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

	_ = container.CreateTopic(ctx, t, "test-topic")
	container.CloseClient(ctx, t)
	// subscription := container.CreateSubscription(ctx, t, "test-topic", "test-subscription")
	connOptions := container.CreateConnectionOptions(t)

	// Set up the buffered publisher.
	bp, err := pubsub.NewBufferedPublisherWithRetryFactory(ctx, "test-project", 1, nil, 3, nil, connOptions...)
	require.NoError(t, err)
	// defer bp.Close()

	// Test publishing a single message.
	err = bp.Publish(ctx, "test-topic", &publisher.MsgPayload{Data: []byte("TestMessage1")})
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	// Test flushing the messages.
	err = bp.Flush(ctx)
	assert.NoError(t, err)

	//Test closing the publisher.
	err = bp.Close(ctx)
	assert.NoError(t, err)
}

func TestPubSubBufferedPublisher_Json(t *testing.T) {
	ctx := context.Background()
	container, err := testcontainer.StartPubSubContainer(ctx, "test-project")
	if err != nil {
		t.Fatal(err)
	}
	// Clean up the container after the test is complete
	t.Cleanup(func() {
		if err := container.StopContainer(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

	_ = container.CreateTopic(ctx, t, "test-topic")
	container.CloseClient(ctx, t)
	// subscription := container.CreateSubscription(ctx, t, "test-topic", "test-subscription")
	connOptions := container.CreateConnectionOptions(t)

	// Set up the buffered publisher.
	bp, err := pubsub.NewPubSubBufferedPublisherFactory(ctx, "test-project", 2, nil, connOptions...)
	require.NoError(t, err)
	// defer bp.Close()

	payload1 := &publisher.MsgPayload{
		MessageId: "message1",
		Data:      []byte("test-message-1"),
		Attributes: map[string]string{
			"attr1": "value1",
			"attr2": "value2",
			"attr3": "value3",
		},
	}

	payload2 := &publisher.MsgPayload{
		MessageId: "message2",
		Data:      []byte("test-message-2"),
		Attributes: map[string]string{
			"attr1": "value1",
			"attr2": "value2",
			"attr3": "value3",
		},
	}

	batch := []publisher.Message{payload1, payload2}

	// Test publishing a single message.
	batchRes, err := bp.Publish(ctx, "test-topic", batch)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	// Test flushing the messages.
	assert.NoError(t, err)

	assert.Len(t, batchRes.Results, 2, "The batch result should be of size %d", 2)

	for _, result := range batchRes.Results {
		assert.Equal(t, result.Success, true)
		assert.Nil(t, result.Err)
	}

	//Test closing the publisher.
	err = bp.Close(ctx)
	assert.NoError(t, err)
}
