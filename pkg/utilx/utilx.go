//nolint:gochecknoglobals
package utilx

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/marcodd23/go-micro-core/pkg/logmgr"
	"github.com/pkg/errors"

	"compress/gzip"

	"github.com/google/uuid"
)

// ParseTimeWithMultipleLayouts parses the time string with the provided layouts or as a numeric timestamp.
func ParseTimeWithMultipleLayouts(s string, layouts ...string) (time.Time, error) {
	// First, try to parse the string as a numeric timestamp
	if timestamp, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(0, timestamp*int64(time.Millisecond)).UTC(), nil
	}

	// If parsing as a timestamp fails, try the provided layouts
	var (
		parsedTime   time.Time
		err          error
		errParseTime error
	)

	for _, layout := range layouts {
		parsedTime, err = time.Parse(layout, s)
		if err == nil {
			return parsedTime.UTC(), nil
		}

		errParseTime = errors.WithMessagef(err, "unable to parse time string with provided layouts: %s", err.Error())
	}

	return time.Time{}, errParseTime
}

// GenerateUUID - generate a UUID.
func GenerateUUID() uuid.UUID {
	for {
		u, err := uuid.NewRandom()
		if err == nil {
			return u
		}
	}
}

// RandomString - Generate a random string of a given length.
func RandomString(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("error generating random string: %w", err)
	}

	return hex.EncodeToString(bytes), nil
}

// ArrayContainsString - check if an array of string contains a given string.
func ArrayContainsString(s *[]string, str string) bool {
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

// GzipDecompressIfNeeded decompresses the data if it's gzipped
func GzipDecompressIfNeeded(ctx context.Context, data []byte) ([]byte, error) {
	if isGzipped(ctx, data) {
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, errors.WithMessage(err, "failed to create gzip reader")
		}

		defer func(reader *gzip.Reader) {
			err := reader.Close()
			if err != nil {
				logmgr.GetLogger().LogError(ctx, "Error closing Gzip Reader")
			}
		}(reader)

		decompressedData, err := io.ReadAll(reader)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to read gzipped data")
		}

		return decompressedData, nil
	}

	return data, nil
}

// isGzipped checks if the data is compressed with Gzip
func isGzipped(ctx context.Context, data []byte) bool {
	contentType := http.DetectContentType(data)
	return contentType == "application/x-gzip"
}

// GzipCompressJSON compresses a JSON-encoded byte slice using gzip.
func GzipCompressJSON(ctx context.Context, jsonData []byte) ([]byte, error) {
	var compressedBuffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressedBuffer)

	_, err := gzipWriter.Write(jsonData)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to write JSON data to gzip writer")
	}

	err = gzipWriter.Close()
	if err != nil {
		return nil, errors.WithMessage(err, "failed to close gzip writer")
	}

	return compressedBuffer.Bytes(), nil
}
