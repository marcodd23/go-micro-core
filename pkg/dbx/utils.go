package dbx

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"math"
	"reflect"

	"github.com/pkg/errors"

	"github.com/marcodd23/go-micro-core/pkg/logx"
)

// GenerateRandomInt64Id generates a random 64-bit ID.
//
// This function generates a random, non-zero 64-bit integer that can be used as a unique identifier for transactions
// or other purposes requiring a unique ID. It uses the crypto/rand package to ensure cryptographic randomness, which
// is important for scenarios where predictable IDs could lead to security vulnerabilities.
//
// The function ensures that the generated ID is non-zero by continuously generating a new ID if the result is zero.
// This prevents issues that could arise from using zero as a special case or invalid ID.
//
// Returns:
//   - int64: A random, non-zero 64-bit integer, which can be used as a unique transaction ID.
//
// Example Usage:
//
//	txID := GenerateRandomInt64Id()
//	log.Printf("Generated transaction ID: %d", txID)
func GenerateRandomInt64Id() int64 {
	var idNum uint64

	for idNum == 0 {
		err := binary.Read(rand.Reader, binary.BigEndian, &idNum)
		if err != nil {
			logx.GetLogger().LogError(context.TODO(), "error generating 64-bit random ID", err)
			continue
		}

		idNum %= uint64(math.MaxInt64)
	}

	return int64(idNum)
}

// DeriveColumnNamesFromTags extracts column names from a struct's tags.
// It uses reflection over the fields of a struct and retrieves the tag values
// specified by `tagKey` (e.g., "db"). Only exported fields (those with an uppercase first letter)
// that contain a non-empty tag and are not marked with `"-"` will be included in the returned slice.
//
// # The purpose of this function is to automatically map struct fields to database column names
//
// Arguments:
//   - entity: The struct from which to derive the column names. Can be a pointer or a value.
//   - tagKey: The key of the tag to extract values from (e.g., "db" for database column mapping).
//
// Returns:
//   - []string: A slice of column names derived from the specified tag on the struct fields.
//   - error: Any error encountered
//
// Example:
//
//	type Example struct {
//	    ID   int    `db:"id"`
//	    Name string `db:"name"`
//	    Age  int    `db:"age"`
//	}
//	columns, _ := DeriveColumnNamesFromTags(Example{}, "db")
//	// columns would be: []string{"id", "name", "age"}
func DeriveColumnNamesFromTags[T any](entity T, tagKey string) ([]string, error) {
	var columnNames []string

	v := reflect.ValueOf(entity)
	t := reflect.TypeOf(entity)

	// Check if it's a pointer, and dereference if necessary
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		t = t.Elem()
	}

	// Ensure that the value is a struct
	if v.Kind() != reflect.Struct {
		return nil, errors.New("expected a struct type")
	}

	// Iterate over each field of the struct
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Get the `db` tag
		dbTag := field.Tag.Get(tagKey)
		if dbTag != "" && dbTag != "-" {
			// Ensure the field is exported (name starts with an uppercase letter)
			if field.PkgPath != "" {
				// Skip unexported fields
				continue
			}

			// Add the tag to the columnNames slice
			columnNames = append(columnNames, dbTag)
		}
	}

	return columnNames, nil
}

// StructsToRows converts a slice of structs to a [][]interface{} for use with pgx.CopyFromRows.
// This uses reflection to extract the values of each struct field.
func StructsToRows[T any](entities []T, tagKey string) ([][]interface{}, error) {
	var rows [][]interface{}

	// Iterate over each entity (struct)
	for _, entity := range entities {
		var row []interface{}

		// Get the struct's value using reflection
		v := reflect.ValueOf(entity)

		// Check if it's a pointer, and dereference if necessary
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		// Ensure that the value is a struct
		if v.Kind() != reflect.Struct {
			return nil, errors.New("expected a struct type")
		}

		// Iterate over each field of the struct
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)

			// Get the `db` tag
			dbTag := field.Tag.Get(tagKey)
			if dbTag != "" && dbTag != "-" {
				// Ensure the field is exported (name starts with an uppercase letter)
				if field.PkgPath != "" {
					// Skip unexported fields
					continue
				}

				// Append the field value to the row
				row = append(row, v.Field(i).Interface())
			}
		}

		// Add the row to the rows slice
		rows = append(rows, row)
	}

	return rows, nil
}
