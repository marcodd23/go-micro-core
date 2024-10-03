package jsonx

import (
	"github.com/goccy/go-json"
	"github.com/pkg/errors"
)

// ParseJSON parses the JSON data into a map
func ParseJSON(jsonData []byte) (map[string]interface{}, error) {
	var event map[string]interface{}
	if err := json.Unmarshal(jsonData, &event); err != nil {
		return nil, errors.WithMessage(err, "failed to parse JSON event")
	}

	return event, nil
}

// ParseJSONIntoStruct parses the JSON data into a map
// target needs to be a pointer to a struct
func ParseJSONIntoStruct[T json.Unmarshaler](jsonData []byte, target T) (T, error) {
	if err := target.UnmarshalJSON(jsonData); err != nil {
		return target, errors.WithMessage(err, "failed to unmarshal JSON data")
	}

	return target, nil
}
