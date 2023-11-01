package messaging

import (
	"log"

	"context"
	"sync"
	"time"
)

// BufferedPublisher - interface for the publisher
type BufferedPublisher interface {
	Publish(ctx context.Context, topicName string, payloadBatch []Message) (*BatchResult, error)
	Close(ctx context.Context) error
}

// BfPublisher - BufferedPublisher struct implementation.
type BfPublisher struct {
	sync.Mutex
	client              Client
	publishConfig       TopicPublishConfig
	batchSize           int32
	flushDelayThreshold time.Duration
	Done                chan struct{}
}

// NewBufferedPublisher - Constructor of BufferedPublisher.
func NewBufferedPublisher(
	client Client,
	publishConfig TopicPublishConfig) (BufferedPublisher, error) {
	bp := &BfPublisher{
		client:              client,
		batchSize:           publishConfig.BatchSize,
		flushDelayThreshold: publishConfig.FlushDelayThreshold,
		Done:                make(chan struct{}),
	}

	return bp, nil
}

// BatchResult - result from batch publishing. It contains reference Id to the original message published.
type BatchResult struct {
	Results []*BufferedPublishResult
}

func (br *BatchResult) appendResult(msgRefId string, success bool, err error) {
	br.Results = append(br.Results, &BufferedPublishResult{MsgRefId: msgRefId, Success: success, Err: err})
}

// BufferedPublishResult - published result.
type BufferedPublishResult struct {
	MsgRefId string
	Success  bool
	Err      error
}

// Publish - Publish a Batch of MessagePayload in Json
func (p *BfPublisher) Publish(ctx context.Context, topicName string, payloadBatch []Message) (*BatchResult, error) {
	p.Lock()
	defer p.Unlock()

	// Non-blocking check if the Done channel is closed
	select {
	case <-p.Done:
		return nil, NewMessagingErrorCode(ErrorPublisherClosed, nil)
	default:
		if int32(len(payloadBatch)) > p.batchSize {
			return nil, NewMessagingError(nil, "error: provide a batch of the configured batch size:  %d", p.batchSize)
		}

		// Initialize a new BatchResult instance with an empty slice of bufferedPublishResult pointers
		batchResult := &BatchResult{
			Results: []*BufferedPublishResult{},
		}

		messagesToPublish := make(map[string]*Message)

		for _, message := range payloadBatch {
			messagesToPublish[message.GetMsgRefId()] = &message
		}

		p.publishBatch(ctx, topicName, messagesToPublish, batchResult)

		return batchResult, nil
	}
}

func (p *BfPublisher) publishBatch(ctx context.Context, topicName string, messages map[string]*Message, batchResult *BatchResult) {
	resultMap := make(map[string]PublishResult)

	topic := p.client.Topic(topicName)
	topic.ConfigPublishSettings(p.publishConfig)

	defer topic.Stop()

	// Publish all messages in the topic and collect the result in the resultMap.
	for msgRefId, msg := range messages {
		resultMap[msgRefId] = topic.Publish(ctx, *msg)
	}

	topic.Flush()

	// Iterate over the resultMap and if there's and build the retryMsgs eventually.
	for msgRefId, res := range resultMap {
		_, err := res.Get(ctx)
		if err != nil {
			log.Printf("failed to publish messages to topic %s: %v, msgRefId: %s", topic.String(), err, msgRefId)
			batchResult.appendResult(msgRefId, false, err)
		} else {
			batchResult.appendResult(msgRefId, true, nil)
		}
	}
}

// Close - close the BufferedPublisher and all the related goroutines.
func (p *BfPublisher) Close(ctx context.Context) error {
	// Non-blocking check if the Done channel is closed
	select {
	case <-p.Done:
		return NewMessagingErrorCode(ErrorPublisherClosed, nil)
	default:
		close(p.Done)

		err := p.client.Close()
		if err != nil {
			return NewMessagingErrorCode(ErrorClosingPubsubClient, err)
		}

		return nil
	}
}
