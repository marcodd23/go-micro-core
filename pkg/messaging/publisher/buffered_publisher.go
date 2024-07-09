package publisher

import (
	"github.com/marcodd23/go-micro-core/pkg/messaging"
	"log"

	"context"
	"sync"
)

// BufferedPublisher - interface for the publisher
type BufferedPublisher interface {
	Publish(ctx context.Context, topicName string, payloadBatch []messaging.Message) (*BatchResult, error)
	Close(ctx context.Context) error
}

// BfPublisher - BufferedPublisher struct implementation.
type BfPublisher struct {
	sync.Mutex
	client        Client
	publishConfig TopicPublishConfig
	Done          chan struct{}
}

// NewBufferedPublisher - Constructor of BufferedPublisher.
func NewBufferedPublisher(
	client Client,
	publishConfig TopicPublishConfig) (BufferedPublisher, error) {
	bp := &BfPublisher{
		client:        client,
		publishConfig: publishConfig,
		Done:          make(chan struct{}),
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

// Publish - Publish a Batch of Messages in Json
func (p *BfPublisher) Publish(ctx context.Context, topicName string, payloadBatch []messaging.Message) (*BatchResult, error) {
	p.Lock()
	defer p.Unlock()

	// Non-blocking check if the Done channel is closed
	select {
	case <-p.Done:
		return nil, NewMessagingErrorCode(ErrorPublisherClosed, nil)
	default:
		if int32(len(payloadBatch)) > p.publishConfig.BatchSize {
			return nil, NewMessagingError(nil, "error: provide a batch of the configured batch size:  %d", p.publishConfig.BatchSize)
		}

		// Initialize a new BatchResult instance with an empty slice of bufferedPublishResult pointers
		batchResult := &BatchResult{
			Results: []*BufferedPublishResult{},
		}

		messagesToPublish := make(map[string]*messaging.Message)

		for _, message := range payloadBatch {
			msg := message
			messagesToPublish[message.GetMsgRefId()] = &msg
		}

		p.publishBatch(ctx, topicName, messagesToPublish, batchResult)

		return batchResult, nil
	}
}

func (p *BfPublisher) publishBatch(ctx context.Context, topicName string, messages map[string]*messaging.Message, batchResult *BatchResult) {
	resultMap := make(map[string]PublishResult)

	topic := p.client.Topic(topicName)
	topic.ConfigPublishSettings(p.publishConfig)

	defer topic.Stop()

	// Publish all messages in the topic and collect the result in the resultMap.
	for msgRefId, message := range messages {
		msg := message
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
