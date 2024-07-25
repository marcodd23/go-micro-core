package pgdb_test

import (
	"github.com/goccy/go-json"
	"github.com/marcodd23/go-micro-core/pkg/database/pgdb"
	"github.com/stretchr/testify/require"
	"testing"
)

// TestPgRowScan_SimpleTypes verifies that PgRowScan can correctly scan and convert simple data types
// (int, string, float64, and bool) into the provided destination variables. It ensures that the values
// are scanned without errors and the scanned values match the expected ones.
func TestPgRowScan_SimpleTypes(t *testing.T) {
	values := []any{1, "example", 3.14, true}
	rowScan := pgdb.PgRowScan{Values: values}

	var id int
	var name string
	var value float64
	var flag bool

	err := rowScan.Scan(&id, &name, &value, &flag)
	require.NoError(t, err)
	require.Equal(t, 1, id)
	require.Equal(t, "example", name)
	require.Equal(t, 3.14, value)
	require.Equal(t, true, flag)
}

// TestPgRowScan_JSONB verifies that PgRowScan can correctly handle JSONB data types. It tests if
// a map[string]interface{} value can be scanned into a []byte variable, and then checks if the
// JSON data is correctly unmarshaled into a map[string]interface{}.
func TestPgRowScan_JSONB(t *testing.T) {
	values := []any{map[string]interface{}{"key": "value"}}
	rowScan := pgdb.PgRowScan{Values: values}

	var jsonData []byte

	err := rowScan.Scan(&jsonData)
	require.NoError(t, err)

	var result map[string]interface{}
	err = json.Unmarshal(jsonData, &result)
	require.NoError(t, err)
	require.Equal(t, "value", result["key"])
}

// TestPgRowScan_TypeMismatch verifies that PgRowScan correctly handles type mismatches by
// attempting to scan values into destination variables with incompatible types. It ensures that
// an error is returned when the types cannot be converted.
func TestPgRowScan_TypeMismatch(t *testing.T) {
	values := []any{1, "example"}
	rowScan := pgdb.PgRowScan{Values: values}

	var id int
	var name int

	err := rowScan.Scan(&id, &name)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot convert")
}

// TestPgRowScan_NilValues verifies that PgRowScan correctly handles nil values. It checks if
// nil values in the input are correctly scanned into nil pointers or zero values of the
// destination variables.
func TestPgRowScan_NilValues(t *testing.T) {
	values := []any{nil, "example"}
	rowScan := pgdb.PgRowScan{Values: values}

	var id *int
	var name string

	err := rowScan.Scan(&id, &name)
	require.NoError(t, err)
	require.Nil(t, id)
	require.Equal(t, "example", name)
}

// TestPgRowScan_PointerTypes verifies that PgRowScan correctly handles pointer types. It ensures
// that values are correctly scanned into pointer variables, and that the pointed-to values match
// the expected ones.
func TestPgRowScan_PointerTypes(t *testing.T) {
	values := []any{1, "example"}
	rowScan := pgdb.PgRowScan{Values: values}

	var id *int
	var name *string

	err := rowScan.Scan(&id, &name)
	require.NoError(t, err)
	require.NotNil(t, id)
	require.Equal(t, 1, *id)
	require.NotNil(t, name)
	require.Equal(t, "example", *name)
}
