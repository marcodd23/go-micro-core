package pipeline

import (
	"context"
	"fmt"
	"github.com/marcodd23/go-micro-core/pkg/logmgr"
)

// A Message is a wrapper for a payload (consisting of a body and attributes) and internal
// message attributes. An instance of PipelineMessage can be routed through a pipeline.
type Message struct {
	messageId  string
	payload    payload
	attributes map[string]interface{}
}

// payload is used in a PipelineMessage to hold the body and public payload attributes of a message.
type payload struct {
	body       []byte
	attributes map[string]string
}

// NewPipelineMessage creates a new PipelineMessage.
func NewPipelineMessage(messageId string, payloadBody []byte, payloadAttrs map[string]string, msgAttrs map[string]interface{}) Message {
	return Message{
		messageId: messageId,
		payload: payload{
			body:       payloadBody,
			attributes: payloadAttrs,
		},
		attributes: msgAttrs}
}

// Copy copies a message and its contents.
func (m Message) Copy() Message {
	return NewPipelineMessage(m.GetMsgRefId(), m.GetPayload(), m.GetPayloadAttributes(), m.GetMessageAttributes())
}

func (m Message) GetMsgRefId() string {
	return m.messageId
}

// GetPayload returns a copy of the message payload.
func (m Message) GetPayload() []byte {
	body := make([]byte, len(m.payload.body))
	copy(body, m.payload.body)

	return body
}

// GetPayloadAttributes returns a copy of the payload attributes.
func (m Message) GetPayloadAttributes() map[string]string {
	copiedMap := make(map[string]string, len(m.payload.attributes))
	for key, value := range m.payload.attributes {
		copiedMap[key] = value
	}

	return copiedMap
}

// GetMessageAttributes returns a copy of the internal message attributes.
func (m Message) GetMessageAttributes() map[string]interface{} {
	copiedMap := make(map[string]interface{}, len(m.attributes))
	for key, value := range m.attributes {
		copiedMap[key] = value
	}

	return copiedMap
}

// AddAttribute adds an internal message attribute to a copy of the original message
// to keep the original instance unchanged.
// It will throw an error if the attribute key already exists.
func (m Message) AddAttribute(key string, value interface{}) (Message, error) {
	// check if attribute already exists
	_, found := m.attributes[key]
	if found {
		return m, fmt.Errorf("attribute key (%v) already exists", key)
	}

	// set attribute and return new message
	msg := m.Copy()
	msg.attributes[key] = value

	return msg, nil
}

// AddPayloadAttribute adds a message payload attribute to a copy of the original message
// to keep the original instance unchanged.
// It will throw an error if the attribute key already exists.
func (m Message) AddPayloadAttribute(key, value string) (Message, error) {
	// check if attribute already exists
	_, found := m.payload.attributes[key]
	if found {
		return m, fmt.Errorf("attribute key (%v) already exists", key)
	}

	// set attribute and return new message
	msg := m.Copy()
	msg.payload.attributes[key] = value

	return msg, nil
}

// UpsertPayloadAttribute adds or updates a message payload attribute in a copy of the original message
// to keep the original instance unchanged.
func (m Message) UpsertPayloadAttribute(key, value string) (Message, error) {
	logmgr.GetLogger().LogDebug(context.Background(), fmt.Sprintf("Upsert GetPayload Attribute key %v", key))

	// set attribute and return new message
	msg := m.Copy()
	msg.payload.attributes[key] = value

	return msg, nil
}
