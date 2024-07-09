package pubsub

// Message - Message GetPayload model implementing Message interface.
type Message struct {
	// MessageId - Pubsub message id
	MessageId string
	// data - Pubsub message payload
	Data []byte
	// Attributes - Pubsub topic Attributes
	Attributes map[string]string
}

// NewPubSubMessage creates a new PipelineMessage.
func NewPubSubMessage(messageId string, payloadBody []byte, payloadAttrs map[string]string) *Message {
	return &Message{
		MessageId:  messageId,
		Data:       payloadBody,
		Attributes: payloadAttrs,
	}
}

// GetMsgRefId - Get message id
func (msg *Message) GetMsgRefId() string {
	return msg.MessageId
}

// GetPayload - Get message payload
func (msg *Message) GetPayload() []byte {
	return msg.Data
}

// GetPayloadAttributes - Get message Attributes
func (msg *Message) GetPayloadAttributes() map[string]string {
	return msg.Attributes
}
