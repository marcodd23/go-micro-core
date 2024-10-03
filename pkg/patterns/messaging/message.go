package messaging

// Message - message payload interface
type Message interface {
	GetMsgRefId() string
	GetPayload() []byte
	GetMessageAttributes() map[string]string
}
