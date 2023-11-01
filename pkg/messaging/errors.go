//nolint:all
package messaging

import "fmt"

// ErrorCode - error code enum.
type ErrorCode int

const (
	ErrorPublisherClosed ErrorCode = iota
	ErrorInitializingPubsubClient
	ErrorConvertingToProto
	ErrorSerializingProtoMessage
	ErrorSerializingJsonMessage
	ErrorClosingPubsubClient
)

var errorMessages = map[ErrorCode]string{
	ErrorPublisherClosed:          "error Publisher is already closed",
	ErrorInitializingPubsubClient: "error initializing Broker Client",
	ErrorConvertingToProto:        "error converting to proto message",
	ErrorSerializingProtoMessage:  "error serializing proto message",
	ErrorSerializingJsonMessage:   "error serializing json message",
	ErrorClosingPubsubClient:      "error closing pubsub client",
}

// Error - General PubSub Error.
type Error struct {
	message string
	err     error
}

// NewMessagingErrorCode - MessagingError constructor given a predefined Error Code.
func NewMessagingErrorCode(code ErrorCode, err error) *Error {
	return &Error{message: errorMessages[code], err: err}
}

// NewMessagingError - NewMessagingError constructor.
func NewMessagingError(err error, msg string, args ...any) *Error {
	return &Error{message: fmt.Sprintf(msg, args...), err: err}
}

func (ge *Error) Error() string {
	if ge.err != nil {
		return fmt.Sprintf("%s: %v", ge.message, ge.err)
	}

	return ge.message
}
