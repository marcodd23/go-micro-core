package publisher

// Message - pubsub message payload interface
type Message interface {
	GetMsgRefId() string
	GetPayload() []byte
	GetAttributes() map[string]string
}

// MsgPayload - MsgPayload Payload model implementing Message interface.
type MsgPayload struct {
	// MessageId - Pubsub message id
	MessageId string
	// Data - Pubsub message payload
	Data []byte
	// Attributes - Pubsub topic attributes
	Attributes map[string]string
}

// GetMsgRefId - Get pubsub message id
func (msg *MsgPayload) GetMsgRefId() string {
	return msg.MessageId
}

// GetPayload - Get pubsub message payload
func (msg *MsgPayload) GetPayload() []byte {
	return msg.Data
}

// GetAttributes - Get pubsub attributes
func (msg *MsgPayload) GetAttributes() map[string]string {
	return msg.Attributes
}
