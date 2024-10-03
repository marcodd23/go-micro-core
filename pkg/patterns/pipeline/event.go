package pipeline

import (
	"fmt"
	"github.com/marcodd23/go-micro-core/pkg/utilx/copyx"
)

// PipeEvent - pipeline message payload interface
type PipeEvent interface {
	GetEventId() string
}

// A ImmutablePipeEvent is a wrapper for a payload (consisting of a body and attributes) and internal
// message attributes. An instance of PipelineMessage can be routed through a pipeline.
type ImmutablePipeEvent struct {
	eventId    string
	payload    []byte
	attributes map[string]string
}

// NewImmutablePipeMessage creates a new ImmutablePipeEvent.
func NewImmutablePipeMessage(eventId string, payloadBody []byte, msgAttrs map[string]string) ImmutablePipeEvent {
	return ImmutablePipeEvent{
		eventId:    eventId,
		payload:    payloadBody,
		attributes: msgAttrs}
}

// Copy copies a message and its contents.
func (m ImmutablePipeEvent) Copy() ImmutablePipeEvent {
	return NewImmutablePipeMessage(m.GetEventId(), m.GetPayload(), m.GetMessageAttributes())
}

func (m ImmutablePipeEvent) GetEventId() string {
	return m.eventId
}

// GetPayload returns a copy of the message payload.
func (m ImmutablePipeEvent) GetPayload() []byte {
	payloadCopy := make([]byte, len(m.payload))
	copyx.DeepCopy(payloadCopy, m.payload)

	return payloadCopy
}

// GetMessageAttributes returns a copy of the payload attributes.
func (m ImmutablePipeEvent) GetMessageAttributes() map[string]string {
	copiedMap := make(map[string]string, len(m.attributes))
	copyx.DeepCopy(copiedMap, m.attributes)

	return copiedMap
}

// AddMessageAttribute adds an internal message attribute to a copy of the original message
// to keep the original instance unchanged.
// It will throw an error if the attribute key already exists.
func (m ImmutablePipeEvent) AddMessageAttribute(key string, value string) (ImmutablePipeEvent, error) {
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
