package messaging

// Message - cloud_pubsub message payload interface
type Message interface {
	GetMsgRefId() string
	GetPayload() []byte
	GetPayloadAttributes() map[string]string
}
