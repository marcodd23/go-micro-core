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

// NewPubSubMessage creates a new PubSub Message.
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

// GetMessageAttributes - Get Message Attributes
func (msg *Message) GetMessageAttributes() map[string]string {
	return msg.Attributes
}
