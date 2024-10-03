package pgxdb_test

import (
	"context"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/marcodd23/go-micro-core/pkg/dbx"
	"github.com/marcodd23/go-micro-core/pkg/dbx/pgxdb"

	"github.com/marcodd23/go-micro-core/test/testcontainer/postgres"
	"github.com/stretchr/testify/require"
)

/*
The Table under test is:

CREATE TABLE EVENT_LOG
(
    MESSAGE_ID         SERIAL PRIMARY KEY,
    ENTITY_NAME        VARCHAR(200) NOT NULL,
    ENTITY_KEY         VARCHAR(200) NOT NULL,
    EVENT_PAYLOAD      JSONB NOT NULL,
    MODIFY_TS          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    AGE                INT,
    IS_ACTIVE          BOOLEAN,
    SALARY             FLOAT8,
    JOIN_DATE          DATE,
	TAGS               TEXT[]
);

*/

// Define a struct matching the EVENT_LOG table schema
type EventLog struct {
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

// Implement the ToRow method for EventLog
func (e EventLog) ToRow() []interface{} {
	return []interface{}{
		e.MessageID,
		e.EntityName,
		e.EntityKey,
		e.EventPayload, // Assuming PostgreSQL can store JSON-like data structures such as JSONB
		e.ModifyTs,
		e.Age,
		e.IsActive,
		e.Salary,
		e.JoinDate,
		e.Tags, // Assuming PostgreSQL can handle arrays (e.g., text[] for tags)
	}
}

// setupTestContainer - setup testcontainer and DB connection manager
func setupTestContainer(ctx context.Context, t *testing.T, prepStatements ...dbx.PreparedStatement) (dbManager dbx.InstanceManager, stopContainer func()) {
	container := postgres.StartPostgresContainer(ctx, t, prepStatements)
	shardManager := postgres.SetupDatabaseConnection(ctx, container)
	db := shardManager.DbShardMap["MAIN_DB"].DbMaster

	// Ensure the database is ready before running tests
	waitForDBReady(ctx, t, db)

	// Return a teardown function to stop the container after the test
	return db, func() {
		container.StopContainer(ctx, t)
	}
}

// waitForDBReady waits for the database container to be ready.
func waitForDBReady(ctx context.Context, t *testing.T, db dbx.InstanceManager) {
	for retries := 0; retries < 20; retries++ {
		_, err := db.Exec(ctx, 0, "SELECT 1")
		if err == nil {
			return
		}
		t.Log(err)
		t.Log("Waiting for database to be ready...")
		time.Sleep(2 * time.Second)
	}

	t.Fatal("Database is not ready after waiting")
}

func TestDatabase(t *testing.T) {
	ctx := context.Background()

	// Define prepared statements
	insertStmt := dbx.NewPreparedStatement(
		"insertEventLog",
		"INSERT INTO EVENT_LOG (entity_name, entity_key, event_payload, age, is_active, salary, join_date, tags) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
	)

	updateStmt := dbx.NewPreparedStatement(
		"updateEventLogAge",
		"UPDATE EVENT_LOG SET age = $1 WHERE entity_name = $2",
	)

	prepStatements := []dbx.PreparedStatement{insertStmt, updateStmt}

	db, stopContainer := setupTestContainer(ctx, t, prepStatements...)
	defer stopContainer()

	// Ensure the database is ready before running tests
	waitForDBReady(ctx, t, db)

	// Insert test data into the database
	insertedData := insertTestData(ctx, db, t)

	t.Run("TestQuery", func(t *testing.T) {
		// Query directly using the Query method of PostgresDB
		query := "SELECT * FROM EVENT_LOG WHERE entity_name = $1"
		conn, rows, err := db.Query(ctx, 0, query, insertedData[0].EntityName)
		require.NoError(t, err)
		defer rows.(pgx.Rows).Close()
		defer conn.(*pgxpool.Conn).Release()

		var eventLog EventLog
		for rows.(pgx.Rows).Next() {
			err := rows.(pgx.Rows).Scan(
				&eventLog.MessageID,
				&eventLog.EntityName,
				&eventLog.EntityKey,
				&eventLog.EventPayload,
				&eventLog.ModifyTs,
				&eventLog.Age,
				&eventLog.IsActive,
				&eventLog.Salary,
				&eventLog.JoinDate,
				&eventLog.Tags,
			)
			require.NoError(t, err)
		}

		require.Equal(t, insertedData[0].EntityName, eventLog.EntityName)
	})

	t.Run("TestQueryAndScan", func(t *testing.T) {
		query := "SELECT * FROM EVENT_LOG WHERE entity_name = $1"
		result, err := pgxdb.QueryAndScan[EventLog](db, ctx, 0, func(rows pgx.Rows) (EventLog, error) {
			var eventLog EventLog
			err := rows.Scan(
				&eventLog.MessageID,
				&eventLog.EntityName,
				&eventLog.EntityKey,
				&eventLog.EventPayload,
				&eventLog.ModifyTs,
				&eventLog.Age,
				&eventLog.IsActive,
				&eventLog.Salary,
				&eventLog.JoinDate,
				&eventLog.Tags,
			)
			return eventLog, err
		}, query, insertedData[0].EntityName)
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Equal(t, insertedData[0].EntityName, result[0].EntityName)
	})

	t.Run("TestQueryScanAndProcess", func(t *testing.T) {
		query := "SELECT * FROM EVENT_LOG WHERE entity_name = $1"
		var processedCount int
		err := pgxdb.QueryScanAndProcess[EventLog](db, ctx, 0, query, func(rows pgx.Rows) (EventLog, error) {
			var eventLog EventLog
			err := rows.Scan(
				&eventLog.MessageID,
				&eventLog.EntityName,
				&eventLog.EntityKey,
				&eventLog.EventPayload,
				&eventLog.ModifyTs,
				&eventLog.Age,
				&eventLog.IsActive,
				&eventLog.Salary,
				&eventLog.JoinDate,
				&eventLog.Tags,
			)
			return eventLog, err
		}, func(item EventLog) error {
			processedCount++
			require.Equal(t, insertedData[0].EntityName, item.EntityName)
			return nil
		}, insertedData[0].EntityName)
		require.NoError(t, err)
		require.Equal(t, 1, processedCount)
	})

	t.Run("TestQueryAndMap", func(t *testing.T) {
		query := "SELECT * FROM EVENT_LOG WHERE entity_name = $1"
		results, err := pgxdb.QueryAndMap[EventLog](db, ctx, 0, query, insertedData[0].EntityName)
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, insertedData[0].EntityName, results[0].EntityName)
	})

	t.Run("TestQueryMapAndProcess", func(t *testing.T) {
		query := "SELECT * FROM EVENT_LOG WHERE entity_name = $1"
		var processedCount int
		err := pgxdb.QueryMapAndProcess[EventLog](db, ctx, 0, query, func(item EventLog) error {
			processedCount++
			require.Equal(t, insertedData[0].EntityName, item.EntityName)
			return nil
		}, insertedData[0].EntityName)
		require.NoError(t, err)
		require.Equal(t, 1, processedCount)
	})

	t.Run("TestExecTransactionalTask", func(t *testing.T) {
		err := pgxdb.ExecTransactionalTask(db, ctx, 0, func(ctx context.Context, tx dbx.Transaction) error {
			query := "UPDATE EVENT_LOG SET age = $1 WHERE entity_name = $2"
			rows, err := tx.TxQuery(ctx, query, 99, insertedData[0].EntityName)

			rows.(pgx.Rows).Close()

			return err
		})
		require.NoError(t, err)

		// Verify the update
		var result []EventLog
		query := "SELECT * FROM EVENT_LOG WHERE entity_name = $1"
		conn, rows, err := db.Query(ctx, 0, query, insertedData[0].EntityName)
		require.NoError(t, err)
		defer rows.(pgx.Rows).Close()
		defer conn.(*pgxpool.Conn).Release()

		for rows.(pgx.Rows).Next() {
			var eventLog EventLog
			err := rows.(pgx.Rows).Scan(
				&eventLog.MessageID,
				&eventLog.EntityName,
				&eventLog.EntityKey,
				&eventLog.EventPayload,
				&eventLog.ModifyTs,
				&eventLog.Age,
				&eventLog.IsActive,
				&eventLog.Salary,
				&eventLog.JoinDate,
				&eventLog.Tags,
			)
			require.NoError(t, err)
			result = append(result, eventLog)
		}

		require.Len(t, result, 1)
		require.Equal(t, 99, result[0].Age)
	})

	t.Run("TestTxQueryAndScan", func(t *testing.T) {
		err := pgxdb.ExecTransactionalTask(db, ctx, 0, func(ctx context.Context, tx dbx.Transaction) error {
			query := "SELECT * FROM EVENT_LOG WHERE entity_name = $1"
			results, err := pgxdb.TxQueryAndScan[EventLog](tx, ctx, func(rows pgx.Rows) (EventLog, error) {
				var eventLog EventLog
				err := rows.Scan(
					&eventLog.MessageID,
					&eventLog.EntityName,
					&eventLog.EntityKey,
					&eventLog.EventPayload,
					&eventLog.ModifyTs,
					&eventLog.Age,
					&eventLog.IsActive,
					&eventLog.Salary,
					&eventLog.JoinDate,
					&eventLog.Tags,
				)
				return eventLog, err
			}, query, insertedData[0].EntityName)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, insertedData[0].EntityName, results[0].EntityName)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("TestTxQueryScanAndProcess", func(t *testing.T) {
		err := pgxdb.ExecTransactionalTask(db, ctx, 0, func(ctx context.Context, tx dbx.Transaction) error {
			query := "SELECT * FROM EVENT_LOG WHERE entity_name = $1"
			var processedCount int
			err := pgxdb.TxQueryScanAndProcess[EventLog](tx, ctx, query, func(rows pgx.Rows) (EventLog, error) {
				var eventLog EventLog
				err := rows.Scan(
					&eventLog.MessageID,
					&eventLog.EntityName,
					&eventLog.EntityKey,
					&eventLog.EventPayload,
					&eventLog.ModifyTs,
					&eventLog.Age,
					&eventLog.IsActive,
					&eventLog.Salary,
					&eventLog.JoinDate,
					&eventLog.Tags,
				)
				return eventLog, err
			}, func(item EventLog) error {
				processedCount++
				require.Equal(t, insertedData[0].EntityName, item.EntityName)
				return nil
			}, insertedData[0].EntityName)
			require.NoError(t, err)
			require.Equal(t, 1, processedCount)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("TestTxQueryAndMap", func(t *testing.T) {
		err := pgxdb.ExecTransactionalTask(db, ctx, 0, func(ctx context.Context, tx dbx.Transaction) error {
			query := "SELECT * FROM EVENT_LOG WHERE entity_name = $1"
			results, err := pgxdb.TxQueryAndMap[EventLog](tx, ctx, query, insertedData[0].EntityName)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, insertedData[0].EntityName, results[0].EntityName)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("TestTxQueryMapAndProcess", func(t *testing.T) {
		err := pgxdb.ExecTransactionalTask(db, ctx, 0, func(ctx context.Context, tx dbx.Transaction) error {
			query := "SELECT * FROM EVENT_LOG WHERE entity_name = $1"
			var processedCount int
			err := pgxdb.TxQueryMapAndProcess[EventLog](tx, ctx, query, func(item EventLog) error {
				processedCount++
				require.Equal(t, insertedData[0].EntityName, item.EntityName)
				return nil
			}, insertedData[0].EntityName)
			require.NoError(t, err)
			require.Equal(t, 1, processedCount)
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("TestTxExecBatch", func(t *testing.T) {
		err := pgxdb.ExecTransactionalTask(db, ctx, 0, func(ctx context.Context, tx dbx.Transaction) error {
			// Create a new batch
			batch := pgxdb.NewEmptyBatch()

			// Queue the INSERT SQL command with Age = 40
			batch.Queue(
				"INSERT INTO EVENT_LOG (entity_name, entity_key, event_payload, age, is_active, salary, join_date, tags) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
				"BatchEntity1", "BatchKey1", `{"key": "value1"}`, 40, true, 6000.50, "2022-01-04", []string{"tag1", "tag2"},
			)

			// Queue the UPDATE SQL command
			batch.Queue(
				"UPDATE EVENT_LOG SET age = $1 WHERE entity_name = $2",
				99, insertedData[0].EntityName,
			)

			// Execute the batch
			rowsAffected, err := pgxdb.TxExecBatch(tx, ctx, batch)
			require.NoError(t, err)
			require.Equal(t, int64(2), rowsAffected) // 1 row inserted, 1 row updated

			return nil
		})
		require.NoError(t, err)

		// Verify the insertion and update
		var result []EventLog
		query := "SELECT * FROM EVENT_LOG WHERE entity_name IN ($1, $2)"
		conn, rows, err := db.Query(ctx, 0, query, "BatchEntity1", insertedData[0].EntityName)
		require.NoError(t, err)
		defer rows.(pgx.Rows).Close()
		defer conn.(*pgxpool.Conn).Release()

		for rows.(pgx.Rows).Next() {
			var eventLog EventLog
			var eventPayloadRaw []byte // Retrieve the raw JSON data
			err := rows.(pgx.Rows).Scan(
				&eventLog.MessageID,
				&eventLog.EntityName,
				&eventLog.EntityKey,
				&eventPayloadRaw, // Read as []byte or json.RawMessage
				&eventLog.ModifyTs,
				&eventLog.Age,
				&eventLog.IsActive,
				&eventLog.Salary,
				&eventLog.JoinDate,
				&eventLog.Tags,
			)
			require.NoError(t, err)

			// Unmarshal the raw JSON payload into the map
			err = json.Unmarshal(eventPayloadRaw, &eventLog.EventPayload)
			require.NoError(t, err)

			result = append(result, eventLog)
		}

		require.Len(t, result, 2)
		// Validate the insertion
		require.Equal(t, "BatchEntity1", result[0].EntityName)
		require.Equal(t, 40, result[0].Age)

		// Access the JSON field after unmarshaling
		require.Equal(t, "value1", result[0].EventPayload["key"])

		require.Equal(t, []string{"tag1", "tag2"}, result[0].Tags)

		// Validate the update
		require.Equal(t, insertedData[0].EntityName, result[1].EntityName)
		require.Equal(t, 99, result[1].Age)

	})

	t.Run("TestPreparedStatements", func(t *testing.T) {
		// Insert a row using the prepared statement
		err := pgxdb.ExecTransactionalTask(db, ctx, 0, func(ctx context.Context, tx dbx.Transaction) error {
			_, err := tx.TxExec(ctx, insertStmt.GetName(), "PreparedEntity1", "PreparedKey1", `{"key": "value1"}`, 30, true, 6000.50, "2022-01-04", []string{"tag1", "tag2"})
			if err != nil {
				return err
			}
			return nil
		})

		require.NoError(t, err)

		// Update a row using the prepared statement
		err = pgxdb.ExecTransactionalTask(db, ctx, 0, func(ctx context.Context, tx dbx.Transaction) error {
			_, err := tx.TxExec(ctx, updateStmt.GetName(), 40, "PreparedEntity1")
			if err != nil {
				return err
			}
			return nil
		})
		require.NoError(t, err)

		// Verify the insertion and update
		var result []EventLog
		query := "SELECT * FROM EVENT_LOG WHERE entity_name = $1"
		conn, rows, err := db.Query(ctx, 0, query, "PreparedEntity1")
		require.NoError(t, err)
		defer rows.(pgx.Rows).Close()
		defer conn.(*pgxpool.Conn).Release()

		for rows.(pgx.Rows).Next() {
			var eventLog EventLog
			var eventPayloadRaw []byte
			err := rows.(pgx.Rows).Scan(
				&eventLog.MessageID,
				&eventLog.EntityName,
				&eventLog.EntityKey,
				&eventPayloadRaw,
				&eventLog.ModifyTs,
				&eventLog.Age,
				&eventLog.IsActive,
				&eventLog.Salary,
				&eventLog.JoinDate,
				&eventLog.Tags,
			)
			require.NoError(t, err)

			// Unmarshal the raw JSON payload into the map
			err = json.Unmarshal(eventPayloadRaw, &eventLog.EventPayload)
			require.NoError(t, err)

			result = append(result, eventLog)
		}

		require.Len(t, result, 1)
		// Validate the insertion
		require.Equal(t, "PreparedEntity1", result[0].EntityName)
		require.Equal(t, 40, result[0].Age)
		require.Equal(t, "value1", result[0].EventPayload["key"])
		require.Equal(t, []string{"tag1", "tag2"}, result[0].Tags)
	})
}

func TestBulkInsertFromStructsWithTags(t *testing.T) {
	ctx := context.Background()

	db, stopContainer := setupTestContainer(ctx, t)
	defer stopContainer()

	// Ensure the database is ready before running tests
	waitForDBReady(ctx, t, db)

	// Define the test data to insert using the EventLog struct
	testData := []EventLog{
		{
			MessageID:    0,
			EntityName:   "BulkEntity1",
			EntityKey:    "BulkKey1",
			EventPayload: map[string]interface{}{"key1": "value1"},
			ModifyTs:     time.Now(),
			Age:          25,
			IsActive:     true,
			Salary:       5000.50,
			JoinDate:     time.Now(),
			Tags:         []string{"tag1", "tag2"},
			TagsNew:      []string{"tag-ignore1", "tag-ignore2"}, // Should be ignored due to `db:"-"` tag
		},
		{
			MessageID:    1,
			EntityName:   "BulkEntity2",
			EntityKey:    "BulkKey2",
			EventPayload: map[string]interface{}{"key2": "value2"},
			ModifyTs:     time.Now(),
			Age:          30,
			IsActive:     false,
			Salary:       6000.75,
			JoinDate:     time.Now(),
			Tags:         []string{"tag3", "tag4"},
			TagsNew:      []string{"tag-ignore3", "tag-ignore4"}, // Should be ignored due to `db:"-"` tag
		},
	}

	// Perform the bulk insert using BulkInsertEntitiesWithTags
	rowCount, err := pgxdb.BulkInsertEntitiesWithTags(db, ctx, 0, "event_log", testData)
	if err != nil {
		t.Logf("Bulk insert failed with error: %v", err)
	}
	require.NoError(t, err)
	require.Equal(t, int64(2), rowCount) // Expect 2 rows to be inserted

	// Query the inserted data to verify the bulk insert worked as expected
	query := "SELECT * FROM EVENT_LOG WHERE entity_name IN ($1, $2)"
	conn, rows, err := db.Query(ctx, 0, query, "BulkEntity1", "BulkEntity2")
	require.NoError(t, err)
	defer rows.(pgx.Rows).Close()
	defer conn.(*pgxpool.Conn).Release()

	var results []EventLog
	for rows.(pgx.Rows).Next() {
		var eventLog EventLog
		var eventPayloadRaw []byte // To handle the raw JSON data
		err := rows.(pgx.Rows).Scan(
			&eventLog.MessageID,
			&eventLog.EntityName,
			&eventLog.EntityKey,
			&eventPayloadRaw, // Read the JSON payload as raw bytes
			&eventLog.ModifyTs,
			&eventLog.Age,
			&eventLog.IsActive,
			&eventLog.Salary,
			&eventLog.JoinDate,
			&eventLog.Tags,
		)
		require.NoError(t, err)

		// Unmarshal the raw JSON payload into the map
		err = json.Unmarshal(eventPayloadRaw, &eventLog.EventPayload)
		require.NoError(t, err)

		results = append(results, eventLog)
	}

	// Ensure 2 rows were retrieved
	require.Len(t, results, 2)

	// Validate the first row
	require.Equal(t, "BulkEntity1", results[0].EntityName)
	require.Equal(t, "BulkKey1", results[0].EntityKey)
	require.Equal(t, map[string]interface{}{"key1": "value1"}, results[0].EventPayload)
	require.Equal(t, 25, results[0].Age)
	require.Equal(t, true, results[0].IsActive)
	require.Equal(t, 5000.50, results[0].Salary)
	require.Equal(t, []string{"tag1", "tag2"}, results[0].Tags)
	require.Nil(t, results[0].TagsNew)

	// Validate the second row
	require.Equal(t, "BulkEntity2", results[1].EntityName)
	require.Equal(t, "BulkKey2", results[1].EntityKey)
	require.Equal(t, map[string]interface{}{"key2": "value2"}, results[1].EventPayload)
	require.Equal(t, 30, results[1].Age)
	require.Equal(t, false, results[1].IsActive)
	require.Equal(t, 6000.75, results[1].Salary)
	require.Equal(t, []string{"tag3", "tag4"}, results[1].Tags)
	require.Nil(t, results[1].TagsNew)
}

// insertTestData inserts predefined rows into the EVENT_LOG table and returns the inserted data.
func insertTestData(ctx context.Context, db dbx.InstanceManager, t *testing.T) []struct {
	EntityName   string
	EntityKey    string
	EventPayload string
	Age          int
	IsActive     bool
	Salary       float64
	JoinDate     string
	Tags         []string
} {
	// Predefined test data for 3 rows
	testData := []struct {
		EntityName   string
		EntityKey    string
		EventPayload string
		Age          int
		IsActive     bool
		Salary       float64
		JoinDate     string
		Tags         []string
	}{
		{"EntityName1", "EntityKey1", `{"key1": "value1"}`, 25, true, 3000.50, "2022-01-01", []string{"tag1-1", "tag1-2"}},
		{"EntityName2", "EntityKey2", `{"key2": "value2"}`, 30, false, 4000.75, "2022-01-02", []string{"tag2-1", "tag2-2"}},
		{"EntityName3", "EntityKey3", `{"key3": "value3"}`, 35, true, 5000.25, "2022-01-03", []string{"tag3-1", "tag3-2"}},
	}

	// Insert the predefined rows into the EVENT_LOG table
	insertQuery := `
		INSERT INTO EVENT_LOG (ENTITY_NAME, ENTITY_KEY, EVENT_PAYLOAD, AGE, IS_ACTIVE, SALARY, JOIN_DATE, TAGS)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	for _, data := range testData {
		_, err := db.Exec(ctx, 0, insertQuery,
			data.EntityName,
			data.EntityKey,
			data.EventPayload,
			data.Age,
			data.IsActive,
			data.Salary,
			data.JoinDate,
			data.Tags)
		require.NoError(t, err, "failed to insert row")
	}

	return testData
}
