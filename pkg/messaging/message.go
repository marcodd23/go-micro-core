package messaging

// Message - message payload interface
type Message interface {
	GetMsgRefId() string
	GetPayload() []byte
	GetPayloadAttributes() map[string]string
}
