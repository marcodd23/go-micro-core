//nolint:gochecknoglobals
package validator

import (
	"errors"

	"github.com/go-playground/validator/v10"
)

// Validator - Validator type.
type Validator struct {
	validate *validator.Validate
}

var validatorInstance *Validator

// NewValidator - Create a new Validator.
func NewValidator() *Validator {
	if validatorInstance == nil {
		validatorInstance = &Validator{validate: validator.New()}
	}

	return validatorInstance
}

// ValidateStruct - apply validation.
func (v *Validator) ValidateStruct(str interface{}) []*ValidationErrorResponse {
	var valErrorsResResult []*ValidationErrorResponse

	err := v.validate.Struct(str)
	if err != nil {
		var validationErrors validator.ValidationErrors
		if errors.As(err, &validationErrors) {
			for _, err := range validationErrors {
				var element ValidationErrorResponse
				element.FailedField = err.StructNamespace()
				element.Tag = err.Tag()
				element.Value = err.Param()
				valErrorsResResult = append(valErrorsResResult, &element)
			}
		}
	}

	return valErrorsResResult
}
