package publisher_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"github.com/marcodd23/go-micro/pkg/messaging/publisher"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestRetryLogicAndBackgroundRoutine_ProtoMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	batchSize := int32(2)
	maxRetryCount := int16(3)
	flushDelayThreshold := time.Millisecond * 10

	publishConfig := publisher.TopicPublishConfig{
		BatchSize:           batchSize,
		MaxRetryCount:       maxRetryCount,
		FlushDelayThreshold: flushDelayThreshold,
	}

	data := []byte("test-message")
	mockMessage := &publisher.MsgPayload{
		Data: data,
		Attributes: map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		},
	}

	var retryCount = int16(0)
	mockResult := MockPublishResult{
		getFunc: func(ctx context.Context) (string, error) {
			retryCount++
			if retryCount <= maxRetryCount {
				return "", errors.New("publish error")
			}
			//Cancel the context
			cancel()
			return "message-id", nil
		},
		readyCh: make(chan struct{}, 1),
	}

	mockTopic := MockTopic{
		id: "test-topic",
		publishFunc: func(ctx context.Context, msg publisher.Message) publisher.PublishResult {
			return mockResult
		},
		configPublishSettingsFunc: func(config func(topic *pubsub.Topic)) {},
	}

	mockClient := &MockClient{
		topics: map[string]publisher.Topic{
			"test-topic": mockTopic,
		},
	}

	bufferedPublisher, err := publisher.NewBufferedPublisherWithRetry(ctx, mockClient, publishConfig)
	assert.NoError(t, err)

	err = bufferedPublisher.Publish(ctx, "test-topic", mockMessage)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				time.Sleep(time.Millisecond * 500)
				return
			case <-time.After(flushDelayThreshold * 50):
				bufferedPublisher.Close(ctx)
				return
			}
		}
	}()

	wg.Wait()

	topicCache, ok := reflect.ValueOf(bufferedPublisher).Elem().FieldByName("TopicCache").Interface().(*publisher.TopicCache)
	assert.Truef(t, ok, "Failed to convert reflect.Value back to *TopicCache")

	topicCache.Lock()
	assert.Empty(t, topicCache.Cache, "topic cache should be empty at the end of the test")
	topicCache.Unlock()

	//Assert we retried 3+1 times before to succeed
	assert.Equal(t, maxRetryCount, retryCount)
}

func TestRetryLogicAndBackgroundRoutine_JsonMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	batchSize := int32(2)
	maxRetryCount := int16(3)
	flushDelayThreshold := time.Millisecond * 10

	publishConfig := publisher.TopicPublishConfig{
		BatchSize:           batchSize,
		MaxRetryCount:       maxRetryCount,
		FlushDelayThreshold: flushDelayThreshold,
	}

	mockMessage := &publisher.MsgPayload{
		Data: []byte("test-message"),
		Attributes: map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		},
	}

	var (
		retryCount      int16
		retryCountMutex sync.Mutex // Mutex to protect the retryCount
	)
	mockResult := MockPublishResult{
		getFunc: func(ctx context.Context) (string, error) {
			retryCountMutex.Lock()
			defer retryCountMutex.Unlock()

			retryCount++
			if retryCount <= maxRetryCount {
				return "", errors.New("publish error")
			}
			//Cancel the context
			cancel()
			return "message-id", nil
		},
		readyCh: make(chan struct{}, 1),
	}

	mockTopic := MockTopic{
		id: "test-topic",
		publishFunc: func(ctx context.Context, msg publisher.Message) publisher.PublishResult {
			return mockResult
		},
		configPublishSettingsFunc: func(config func(topic *pubsub.Topic)) {},
	}

	mockClient := &MockClient{
		topics: map[string]publisher.Topic{
			"test-topic": mockTopic,
		},
	}

	bufferedPublisher, err := publisher.NewBufferedPublisherWithRetry(ctx, mockClient, publishConfig)
	assert.NoError(t, err)

	err = bufferedPublisher.Publish(ctx, "test-topic", mockMessage)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				time.Sleep(time.Millisecond * 500)
				return
			case <-time.After(flushDelayThreshold * 50):
				bufferedPublisher.Close(ctx)
				return
			}
		}
	}()

	wg.Wait()

	//Assert we retried 3+1 times before to succeed
	retryCountMutex.Lock()
	assert.Equal(t, maxRetryCount, retryCount)
	retryCountMutex.Unlock()
}

func TestClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	batchSize := int32(1)
	maxRetryCount := int16(3)
	flushDelayThreshold := time.Millisecond * 10

	publishConfig := publisher.TopicPublishConfig{
		BatchSize:           batchSize,
		MaxRetryCount:       maxRetryCount,
		FlushDelayThreshold: flushDelayThreshold,
	}

	mockMessage := &publisher.MsgPayload{
		Data: []byte("test-message"),
		Attributes: map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		},
	}

	mockResult := MockPublishResult{
		getFunc: func(ctx context.Context) (string, error) {
			return "message-id", nil
		},
		readyCh: make(chan struct{}, 1),
	}

	mockTopic := MockTopic{
		id: "test-topic",
		publishFunc: func(ctx context.Context, msg publisher.Message) publisher.PublishResult {
			return mockResult
		},
		configPublishSettingsFunc: func(config func(topic *pubsub.Topic)) {},
	}

	mockClient := &MockClient{
		topics: map[string]publisher.Topic{
			"test-topic": mockTopic,
		},
	}

	bufferedPublisher, err := publisher.NewBufferedPublisherWithRetry(ctx, mockClient, publishConfig)
	assert.NoError(t, err)

	err = bufferedPublisher.Publish(ctx, "test-topic", mockMessage)
	assert.NoError(t, err)

	// Close the BufferedPublisher
	err = bufferedPublisher.Close(ctx)
	assert.NoError(t, err)

	// Check if the background routines have stopped by calling Flush after Close
	err = bufferedPublisher.Flush(ctx)
	assert.Error(t, err, "Flush should return an error after Close")
}
