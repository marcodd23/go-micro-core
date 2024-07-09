//nolint:gochecknoglobals
package app_utils

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/goccy/go-json"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

var sourceTableNameError = errors.New("table name parsing failed")

func tableNameParsingError(fileName string) error {
	return fmt.Errorf("%w, file name %s", sourceTableNameError, fileName)
}

// Utils - Utils type.
type Utils struct{}

var utils *Utils

// NewUtils - create a new Utils type.
func NewUtils() *Utils {
	if utils == nil {
		utils = &Utils{}
	}

	return utils
}

// ParseTimeWithMultipleLayouts - parse the tine with the provided layouts.
func (u *Utils) ParseTimeWithMultipleLayouts(s string, layouts ...string) (time.Time, error) {
	var (
		parsedTime time.Time
		err        error
	)

	var errParseTime error

	for _, layout := range layouts {
		parsedTime, err = time.Parse(layout, s)
		if err == nil {
			return parsedTime.UTC(), nil
		}

		errParseTime = fmt.Errorf("unable to parse time string with provided layouts: %w", err)
	}

	return time.Time{}, errParseTime
}

// GenerateUUID - generate a UUID.
func (u *Utils) GenerateUUID() uuid.UUID {
	for {
		u, err := uuid.NewRandom()
		if err == nil {
			return u
		}
	}
}

// RandomString - Generate a random string of a given length.
func (u *Utils) RandomString(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("error generating random string: %w", err)
	}

	return hex.EncodeToString(bytes), nil
}

// ArrayContainsString - check if an array of string contains a given string.
func (u *Utils) ArrayContainsString(s *[]string, str string) bool {
	if s != nil {
		for _, v := range *s {
			if v == str {
				return true
			}
		}
	}

	return false
}

// FetchEnvVar - fetch env var as string.
func FetchEnvVar(envName string) string {
	val := os.Getenv(envName)
	if val == "" {
		log.Panicf("Error: %s Env Variable not Set", envName)
	}

	return val
}

// FetchEnvVarAsInt64 - fetch env var as int64.
func FetchEnvVarAsInt64(envName string) int64 {
	val := FetchEnvVar(envName)

	result, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		log.Panicf("Error parsing %s env variable: %v", envName, err)
	}

	return result
}

// FetchEnvVarAsInt16 - fetch env var as int16.
func FetchEnvVarAsInt16(envName string) int16 {
	val := FetchEnvVar(envName)

	result64, err := strconv.ParseInt(val, 10, 16)
	if err != nil {
		log.Panicf("Error parsing %s env variable: %v", envName, err)
	}

	return int16(result64)
}

// FetchEnvVarAsTime - fetch evn var as time.
func FetchEnvVarAsTime(envName string, layout string) time.Time {
	val := FetchEnvVar(envName)
	val = strings.Trim(val, "\"")

	t, err := time.Parse(layout, val)
	if err != nil {
		log.Panicf("Error parsing %s env variable: %v", envName, err)
	}

	return t
}

// ParseJson - Parse json into generic map
func (u *Utils) ParseJson(attunityMessage *string) (map[string]any, error) {
	attunityMessageMap := map[string]any{}
	err := json.Unmarshal([]byte(*attunityMessage), &attunityMessageMap)

	if err != nil {
		return nil, fmt.Errorf("ParseJson failed: %w", err)
	}

	return attunityMessageMap, nil
}
