package errorx

import (
	"fmt"
)

// GENERAL ERROR:

// GeneralError - General App Error.
type GeneralError struct {
	message string
	err     error
}

// NewGeneralError - GeneralError constructor.
func NewGeneralError(msg string, args ...any) *GeneralError {
	return &GeneralError{message: fmt.Sprintf(msg, args...), err: nil}
}

// NewGeneralErrorWrapper - GeneralError constructor for wrapper of another error.
func NewGeneralErrorWrapper(err error, msg string, args ...any) *GeneralError {
	return &GeneralError{message: fmt.Sprintf(msg, args...), err: err}
}

// Error - return the error string.
func (ge *GeneralError) Error() string {
	if ge.err != nil {
		return fmt.Errorf("%s # Error wrap: %w", ge.message, ge.err).Error()
	}

	return ge.message
}

// DATABASE ERROR

// DatabaseError - General App Error.
type DatabaseError struct {
	message string
	err     error
}

// NewDatabaseError - GeneralError constructor.
func NewDatabaseError(msg string, args ...any) *DatabaseError {
	return &DatabaseError{message: fmt.Sprintf(msg, args...), err: nil}
}

// NewDatabaseErrorWrapper - GeneralError constructor for wrapper of another error.
func NewDatabaseErrorWrapper(err error, msg string, args ...any) *DatabaseError {
	return &DatabaseError{message: fmt.Sprintf(msg, args...), err: err}
}

// Error - return the error string.
func (ge *DatabaseError) Error() string {
	if ge.err != nil {
		return fmt.Errorf("%s: %w", ge.message, ge.err).Error()
	}

	return ge.message
}
