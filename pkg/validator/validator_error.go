package validator

import (
	"context"
	"github.com/goccy/go-json"
	"github.com/marcodd23/go-micro-core/pkg/logmgr"
)

// ValidationError - Errors for tags validation.
type ValidationError struct {
	errors []*ValidationErrorResponse
}

// ValidationErrorResponse - Struct for the validation error.
type ValidationErrorResponse struct {
	FailedField string
	Tag         string
	Value       string
}

// NewValidationError - ValidationError constructor.
func NewValidationError(errors []*ValidationErrorResponse) *ValidationError {
	return &ValidationError{errors: errors}
}

func (v *ValidationError) Error() string {
	data, err := json.Marshal(v)
	if err != nil {
		logmgr.GetLogger().LogError(context.TODO(), "Error marshalling -Validation Error- to JSON:", err)
		return ""
	}

	return string(data)
}

// GetErrorsDetails - return the errors.
func (v *ValidationError) GetErrorsDetails() []*ValidationErrorResponse {
	return v.errors
}
