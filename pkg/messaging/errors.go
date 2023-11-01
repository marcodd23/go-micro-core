//nolint:all
package messaging

import "fmt"

// MessagingErrorCode
type MessagingErrorCode int

const (
	ErrorPublisherClosed MessagingErrorCode = iota
	ErrorInitializingPubsubClient
	ErrorConvertingToProto
	ErrorSerializingProtoMessage
	ErrorSerializingJsonMessage
	ErrorClosingPubsubClient
)

var errorMessages = map[MessagingErrorCode]string{
	ErrorPublisherClosed:          "error Publisher is already closed",
	ErrorInitializingPubsubClient: "error initializing Broker Client",
	ErrorConvertingToProto:        "error converting to proto message",
	ErrorSerializingProtoMessage:  "error serializing proto message",
	ErrorSerializingJsonMessage:   "error serializing json message",
	ErrorClosingPubsubClient:      "error closing pubsub client",
}

// MessagingError - General PubSub Error.
type MessagingError struct {
	message string
	err     error
}

// NewMessagingErrorCode - MessagingError constructor given a predefined Error Code.
func NewMessagingErrorCode(code MessagingErrorCode, err error) *MessagingError {
	return &MessagingError{message: errorMessages[code], err: err}
}

// NewMessagingError - NewMessagingError constructor.
func NewMessagingError(err error, msg string, args ...any) *MessagingError {
	return &MessagingError{message: fmt.Sprintf(msg, args...), err: err}
}

func (ge *MessagingError) Error() string {
	if ge.err != nil {
		return fmt.Sprintf("%s: %v", ge.message, ge.err)
	}

	return ge.message
}
