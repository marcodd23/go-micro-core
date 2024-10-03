package publisher

import (
	"context"
	"fmt"
	"github.com/marcodd23/go-micro-core/pkg/patterns/messaging"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marcodd23/go-micro-core/pkg/logx"
)

// BufferedPublisherWithRetry - interface for the publisher
type BufferedPublisherWithRetry interface {
	Publish(ctx context.Context, topic string, message messaging.Message) error
	Flush(ctx context.Context) error
	Close(ctx context.Context) error
	GetBufferedMessages(topic string) []*messaging.Message
}

// TopicCache - Topic Cache with mutex access.
type TopicCache struct {
	sync.Mutex
	Cache map[string]*TopicCacheItem
}

// TopicCacheItem - topic cache item.
type TopicCacheItem struct {
	topic        Topic
	usageCounter atomic.Int32
}

// NewTopicCacheItem -
func NewTopicCacheItem(topic Topic, initialValue int32) *TopicCacheItem {
	cacheItem := &TopicCacheItem{
		topic: topic,
	}
	cacheItem.usageCounter.Store(initialValue)

	return cacheItem
}

type retryMsgs []*messaging.Message

type retryBatch struct {
	topic     Topic
	topicName string
	messages  retryMsgs
	count     int16
}

// bufferedPublisherWithRetry - buffered publisher struct implementation.
type bufferedPublisherWithRetry struct {
	sync.Mutex
	client               Client
	batchSize            int32
	flushDelayThreshold  time.Duration
	maxRetryCount        int16
	initialRetryInterval time.Duration
	publishConfig        TopicPublishConfig
	bufferedMessages     map[string][]*messaging.Message // bufferedMessages by topic.
	TopicCache           *TopicCache
	Done                 chan struct{}
	RetryCh              chan retryBatch
}

// NewBufferedPublisherWithRetry - Constructor.
func NewBufferedPublisherWithRetry(
	ctx context.Context,
	client Client,
	publishConfig TopicPublishConfig) (BufferedPublisherWithRetry, error) {
	topicCache := &TopicCache{
		Cache: make(map[string]*TopicCacheItem),
	}

	bp := &bufferedPublisherWithRetry{
		client:               client,
		batchSize:            publishConfig.BatchSize,
		maxRetryCount:        publishConfig.MaxRetryCount,
		flushDelayThreshold:  publishConfig.FlushDelayThreshold,
		initialRetryInterval: publishConfig.InitialRetryInterval,
		bufferedMessages:     make(map[string][]*messaging.Message),
		TopicCache:           topicCache,
		Done:                 make(chan struct{}),
		RetryCh:              make(chan retryBatch),
	}

	bp.startBackgroundRoutines(ctx)

	return bp, nil
}

// Publish - publish a message in Json format and buffer it in an internal buffer of a given size, before Flushing in batch.
// The batching mechanism is abstracted from the user that just need to publish one message at time.
func (p *bufferedPublisherWithRetry) Publish(ctx context.Context, topic string, message messaging.Message) error {
	p.Lock()
	defer p.Unlock()

	// Non-blocking check if the Done channel is closed
	select {
	case <-p.Done:
		return NewMessagingErrorCode(ErrorPublisherClosed, nil)
	default:
		p.bufferedMessages[topic] = append(p.bufferedMessages[topic], &message)

		if int32(len(p.bufferedMessages[topic])) >= p.batchSize {
			return p.flushTopic(ctx, topic)
		}

		return nil
	}
}

// Close - close the BufferedPublisher and all the related goroutines.
func (p *bufferedPublisherWithRetry) Close(ctx context.Context) error {

	// Non-blocking check if the Done channel is closed
	select {
	case <-p.Done:
		return NewMessagingErrorCode(ErrorPublisherClosed, nil)
	default:
		// Flush the remaining buffer
		var flushRetry int16 = 0
		for {
			totalRemainingMessages := p.getTotalRemainingMessagesCountInBuffer()
			if totalRemainingMessages <= 0 || flushRetry >= p.maxRetryCount {
				break
			}

			logx.GetLogger().LogInfo(ctx, fmt.Sprintf("Flushing %d remaining messages before closing PubSub Client", totalRemainingMessages))
			err := p.Flush(ctx)
			if err != nil {
				logx.GetLogger().LogError(ctx, "Error final flushing of PubSub message buffer", err)
			}
			flushRetry += 1
		}

		close(p.Done)

		err := p.client.Close()
		if err != nil {
			return NewMessagingErrorCode(ErrorClosingPubsubClient, err)
		}

		return nil
	}
}

func (p *bufferedPublisherWithRetry) getTotalRemainingMessagesCountInBuffer() int {
	var remainingMessages = 0
	for _, topicBuffer := range p.bufferedMessages {
		remainingMessages += len(topicBuffer)
	}

	return remainingMessages
}

// Background goroutines to handle periodic flush and retries.
func (p *bufferedPublisherWithRetry) startBackgroundRoutines(ctx context.Context) {
	// Start a goroutine for periodic flushing
	go func() {
		ticker := time.NewTicker(p.flushDelayThreshold)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				err := p.Flush(ctx)
				if err != nil {
					log.Printf("Error flushing: %v", err)
				}
			case <-p.Done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	// Start a goroutine for handling retries
	go func() {
		for {
			select {
			case batch := <-p.RetryCh:
				p.retryHandler(ctx, batch)
			case <-p.Done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (p *bufferedPublisherWithRetry) retryHandler(ctx context.Context, batch retryBatch) {
	if batch.count < p.maxRetryCount {
		log.Printf("Retrying batch (attempt %d) for topic: %s", batch.count+1, batch.topic)
		// calculates an exponential delay factor by left-shifting 1 by batch.count times.
		// In other words, it computes 2^(batch.count).
		time.Sleep(p.initialRetryInterval * time.Duration(1<<batch.count))
		failedMsgs := p.publishBatch(ctx, batch.topic, batch.messages)

		if len(failedMsgs) > 0 {
			go func() {
				p.RetryCh <- retryBatch{
					topic:     batch.topic,
					topicName: batch.topicName,
					messages:  failedMsgs,
					count:     batch.count + 1,
				}
			}()
		}
	} else {
		log.Printf("Max retry threshold reached (%d) for topic: %s. Messages: %v", p.maxRetryCount, batch.topic.String(), batch.messages)

		// release the topic.
		err := p.releaseTopicFromCache(batch.topicName)
		if err != nil {
			log.Printf("error: %s", err.Error())
		}
	}
}

// Flush all the messages for all the topics.
func (p *bufferedPublisherWithRetry) Flush(ctx context.Context) error {
	p.Lock()
	defer p.Unlock()

	// Non-blocking check if the Done channel is closed
	select {
	case <-p.Done:
		return NewMessagingErrorCode(ErrorPublisherClosed, nil)
	default:
		for topic := range p.bufferedMessages {
			if err := p.flushTopic(ctx, topic); err != nil {
				return NewMessagingError(err, "error flushing topic %s", topic)
			}
		}
	}

	return nil
}

func (p *bufferedPublisherWithRetry) flushTopic(ctx context.Context, topicName string) error {
	msgToPublish := p.bufferedMessages[topicName]
	if len(msgToPublish) == 0 {
		return nil
	}

	pubsubTopic := p.acquireTopicFromCache(topicName)

	failedMsgs := p.publishBatch(ctx, pubsubTopic, msgToPublish)

	// If there are failed Messages, chain them in the Retry Channel to be reprocessed.
	if len(failedMsgs) > 0 {
		p.RetryCh <- retryBatch{
			topic:     pubsubTopic,
			topicName: topicName,
			messages:  failedMsgs,
			count:     1,
		}
	} else {
		err := p.releaseTopicFromCache(topicName)
		if err != nil {
			return fmt.Errorf("error: %w", err)
		}
	}

	// Reset the buffer.
	p.bufferedMessages[topicName] = p.bufferedMessages[topicName][:0]

	return nil
}

func (p *bufferedPublisherWithRetry) acquireTopicFromCache(topicId string) Topic {
	p.TopicCache.Lock()
	defer p.TopicCache.Unlock()

	var cacheItem *TopicCacheItem

	cacheItem = p.TopicCache.Cache[topicId]
	if cacheItem != nil {
		cacheItem.usageCounter.Add(1)
	} else {
		topic := p.client.Topic(topicId)
		topic.ConfigPublishSettings(p.publishConfig)

		cacheItem = NewTopicCacheItem(topic, 1)

		p.TopicCache.Cache[topicId] = cacheItem
	}

	return cacheItem.topic
}

func (p *bufferedPublisherWithRetry) releaseTopicFromCache(topic string) error {
	p.TopicCache.Lock()
	defer p.TopicCache.Unlock()

	cacheItem := p.TopicCache.Cache[topic]

	if cacheItem == nil {
		log.Println("error releasing topic from cache. Topic not found in cache")
		return NewMessagingError(nil, "error releasing topic from cache. Topic not found in cache")
	} else {
		cacheItem.usageCounter.Add(-1)

		if cacheItem.usageCounter.Load() == 0 {
			// remove cache item.
			// stop topic.
			delete(p.TopicCache.Cache, topic)
			cacheItem.topic.Stop()
		}
	}

	return nil
}

// GetBufferedMessages - get hte messages in the buyffer. Useful for testing.
func (p *bufferedPublisherWithRetry) GetBufferedMessages(topic string) []*messaging.Message {
	p.Lock()
	defer p.Unlock()

	return p.bufferedMessages[topic]
}

func (p *bufferedPublisherWithRetry) publishBatch(ctx context.Context, topic Topic, messages []*messaging.Message) retryMsgs {
	var retryMsgs retryMsgs

	resultMap := make(map[*messaging.Message]PublishResult)

	// Publish all messages in the topic and collect the result in the resultMap.
	for _, message := range messages {
		msg := message
		resultMap[msg] = topic.Publish(ctx, *msg)
	}

	topic.Flush()

	// Iterate over the resultMap and if there's and build the retryMsgs eventually.
	for msg, res := range resultMap {
		_, err := res.Get(ctx)
		if err != nil {
			log.Printf("failed to publish messages to topic %s: %v, messages: %v", topic.String(), err, msg)
			retryMsgs = append(retryMsgs, msg)
		}
	}

	return retryMsgs
}
