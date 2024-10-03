//nolint:gochecknoglobals
package utilx

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

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
