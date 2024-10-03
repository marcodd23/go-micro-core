package dbx_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/marcodd23/go-micro-core/pkg/dbx"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

// Define a struct matching the EVENT_LOG table schema
type TestStruct struct {
	MessageID    int                    `db:"message_id"`
	EntityName   string                 `db:"entity_name"`
	EntityKey    string                 `db:"entity_key"`
	EventPayload map[string]interface{} `db:"event_payload"`
	ModifyTs     time.Time              `db:"modify_ts"`
	Age          int                    `db:"age"`
	IsActive     bool                   `db:"is_active"`
	Salary       float64                `db:"salary"`
	JoinDate     time.Time              `db:"join_date"`
	Tags         []string               `db:"tags"`
	TagsNew      []string               `db:"-"`
}

func TestDeriveColumnNamesFromTags(t *testing.T) {
	// Call DeriveColumnNamesFromTags with the TestStruct
	columns, err := dbx.DeriveColumnNamesFromTags(TestStruct{}, "db")

	// Expected column names from the `db` tags
	expectedColumns := []string{
		"message_id",
		"entity_name",
		"entity_key",
		"event_payload",
		"modify_ts",
		"age",
		"is_active",
		"salary",
		"join_date",
		"tags",
	}

	// Assert that there were no errors
	assert.NoError(t, err)

	// Assert that the columns are as expected
	assert.True(t, reflect.DeepEqual(expectedColumns, columns), "Expected %v but got %v", expectedColumns, columns)
}

func TestStructsToRows(t *testing.T) {
	// Define some test data using the EventLog struct
	testData := []TestStruct{
		{
			MessageID:    1,
			EntityName:   "TestEntity1",
			EntityKey:    "TestKey1",
			EventPayload: map[string]interface{}{"key1": "value1"},
			ModifyTs:     time.Now(),
			Age:          25,
			IsActive:     true,
			Salary:       5000.50,
			JoinDate:     time.Now(),
			Tags:         []string{"tag1", "tag2"},
			TagsNew:      []string{"tag1", "tag2"}, // Should be ignored because of `db:"-"` tag
		},
		{
			MessageID:    2,
			EntityName:   "TestEntity2",
			EntityKey:    "TestKey2",
			EventPayload: map[string]interface{}{"key2": "value2"},
			ModifyTs:     time.Now(),
			Age:          30,
			IsActive:     false,
			Salary:       6000.75,
			JoinDate:     time.Now(),
			Tags:         []string{"tag3", "tag4"},
			TagsNew:      []string{"tag3", "tag4"}, // Should be ignored because of `db:"-"` tag
		},
	}

	// Call structsToRows to convert the test data into [][]interface{}
	rows, err := dbx.StructsToRows(testData, "db")
	require.NoError(t, err)

	// Verify that the correct number of rows were created
	require.Len(t, rows, 2)

	// Validate the first row's data
	require.Len(t, rows[0], 10)                                            // We expect 10 columns because TagsNew is ignored
	require.Equal(t, 1, rows[0][0])                                        // MessageID
	require.Equal(t, "TestEntity1", rows[0][1])                            // EntityName
	require.Equal(t, "TestKey1", rows[0][2])                               // EntityKey
	require.Equal(t, map[string]interface{}{"key1": "value1"}, rows[0][3]) // EventPayload
	require.Equal(t, 25, rows[0][5])                                       // Age
	require.Equal(t, true, rows[0][6])                                     // IsActive
	require.Equal(t, 5000.50, rows[0][7])                                  // Salary
	require.Equal(t, []string{"tag1", "tag2"}, rows[0][9])                 // Tags

	// Validate the second row's data
	require.Len(t, rows[1], 10)                                            // We expect 10 columns because TagsNew is ignored
	require.Equal(t, 2, rows[1][0])                                        // MessageID
	require.Equal(t, "TestEntity2", rows[1][1])                            // EntityName
	require.Equal(t, "TestKey2", rows[1][2])                               // EntityKey
	require.Equal(t, map[string]interface{}{"key2": "value2"}, rows[1][3]) // EventPayload
	require.Equal(t, 30, rows[1][5])                                       // Age
	require.Equal(t, false, rows[1][6])                                    // IsActive
	require.Equal(t, 6000.75, rows[1][7])                                  // Salary
	require.Equal(t, []string{"tag3", "tag4"}, rows[1][9])                 // Tags
}
