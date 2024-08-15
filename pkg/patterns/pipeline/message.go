package pipeline

import (
	"fmt"
)

// A Message is a wrapper for a payload (consisting of a body and attributes).
// An instance of Message can be routed through a pipeline.
type Message interface {
	GetMsgRefId() string
	GetPayload() []byte
	GetMessageAttributes() map[string]string
}

type PipeMessage struct {
	messageId  string
	payload    []byte
	attributes map[string]string
}

// NewPipeMessage creates a new PipelineMessage.
func NewPipeMessage(messageId string, payloadBody []byte, msgAttrs map[string]string) PipeMessage {
	return PipeMessage{
		messageId:  messageId,
		payload:    payloadBody,
		attributes: msgAttrs}
}

func (m PipeMessage) GetMsgRefId() string {
	return m.messageId
}

// GetPayload returns the message payload.
func (m PipeMessage) GetPayload() []byte {
	return m.payload
}

// GetMessageAttributes returns a copy of the payload attributes.
func (m PipeMessage) GetMessageAttributes() map[string]string {
	return m.attributes
}

// AddMessageAttribute adds an internal message attribute to a copy of the original message
// to keep the original instance unchanged.
// It will throw an error if the attribute key already exists.
func (m PipeMessage) AddMessageAttribute(key string, value string) (PipeMessage, error) {
	// check if attribute already exists
	_, found := m.attributes[key]
	if found {
		return m, fmt.Errorf("attribute key (%v) already exists", key)
	}

	// set attribute and return new message
	m.attributes[key] = value

	return m, nil
}

// A ImmutablePipeMessage is a wrapper for a payload (consisting of a body and attributes) and internal
// message attributes. An instance of PipelineMessage can be routed through a pipeline.
type ImmutablePipeMessage PipeMessage

// NewImmutablePipeMessage creates a new ImmutablePipeMessage.
func NewImmutablePipeMessage(messageId string, payloadBody []byte, msgAttrs map[string]string) ImmutablePipeMessage {
	return ImmutablePipeMessage{
		messageId:  messageId,
		payload:    payloadBody,
		attributes: msgAttrs}
}

// Copy copies a message and its contents.
func (m ImmutablePipeMessage) Copy() ImmutablePipeMessage {
	return NewImmutablePipeMessage(m.GetMsgRefId(), m.GetPayload(), m.GetMessageAttributes())
}

func (m ImmutablePipeMessage) GetMsgRefId() string {
	return m.messageId
}

// GetPayload returns a copy of the message payload.
func (m ImmutablePipeMessage) GetPayload() []byte {
	payloadCopy := make([]byte, len(m.payload))
	copy(payloadCopy, m.payload)

	return payloadCopy
}

// GetMessageAttributes returns a copy of the payload attributes.
func (m ImmutablePipeMessage) GetMessageAttributes() map[string]string {
	copiedMap := make(map[string]string, len(m.attributes))
	for key, value := range m.attributes {
		copiedMap[key] = value
	}

	return copiedMap
}

// AddMessageAttribute adds an internal message attribute to a copy of the original message
// to keep the original instance unchanged.
// It will throw an error if the attribute key already exists.
func (m ImmutablePipeMessage) AddMessageAttribute(key string, value string) (ImmutablePipeMessage, error) {
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
