package pgdb_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/marcodd23/go-micro-core/pkg/database"
	testcontainer "github.com/marcodd23/go-micro-core/test/testcontainer/testcontainer_pg"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

/*
The Table under test is:

CREATE TABLE EVENT_LOG
(
    MESSAGE_ID         SERIAL PRIMARY KEY,
    ENTITY_NAME        VARCHAR(200) NOT NULL,
    ENTITY_KEY         VARCHAR(200) NOT NULL,
    EVENT_PAYLOAD      JSONB NOT NULL,
    MODIFY_TS          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

*/

// waitForDBReady waits for the database container to be ready.
func waitForDBReady(ctx context.Context, t *testing.T, db database.InstanceManager) {
	for retries := 0; retries < 20; retries++ {
		_, err := db.Exec(ctx, 0, "SELECT 1")
		if err == nil {
			return
		}
		t.Log("Waiting for database to be ready...")
		time.Sleep(2 * time.Second)
	}
	t.Fatal("Database is not ready after waiting")
}

func TestDatabase(t *testing.T) {
	ctx := context.Background()
	container := testcontainer.StartPostgresContainer(ctx, t, nil)
	defer container.StopContainer(ctx, t)

	shardManager := testcontainer.SetupDatabaseConnection(ctx, container)
	db := shardManager.DbShardMap["MAIN_DB"].DbMaster

	// Create a cleanup function to restore the snapshot after each sub-test
	cleanup := func() {
		err := container.Container.Restore(ctx, postgres.WithSnapshotName(testcontainer.TestSnapshotId))
		require.NoError(t, err)
	}

	t.Run("TestQuery", func(t *testing.T) {
		defer cleanup()

		waitForDBReady(ctx, t, db)

		// Insert multiple rows into the EVENT_LOG table with varying fields
		insertQuery := `
			INSERT INTO EVENT_LOG (ENTITY_NAME, ENTITY_KEY, EVENT_PAYLOAD)
			VALUES ($1, $2, $3)
		`
		for i := 1; i <= 10; i++ {
			_, err := db.Exec(ctx, 0, insertQuery, fmt.Sprintf("EntityName%d", i), fmt.Sprintf("EntityKey%d", i), fmt.Sprintf(`{"key%d": "value%d"}`, i, i))
			require.NoError(t, err, "failed to insert row")
		}

		// Query the rows back from the EVENT_LOG table
		selectQuery := `
			SELECT MESSAGE_ID, ENTITY_NAME, ENTITY_KEY, EVENT_PAYLOAD, MODIFY_TS
			FROM EVENT_LOG
			ORDER BY MESSAGE_ID
		`
		resultSet, err := db.Query(ctx, 0, selectQuery)
		require.NoError(t, err, "failed to query rows")

		// Close the resultset
		defer resultSet.Close()

		// Ensure we have 10 rows in the resultSet
		require.Equal(t, 10, len(resultSet.GetRows()), "expected 10 rows in resultSet")
		require.Equal(t, 10, len(resultSet.GetRowsScan()), "expected 10 rows in RowsScan")

		// Verify each row content using resultSet.GetRow(i)
		for i := 0; i < 10; i++ {
			row, err := resultSet.GetRow(i)
			require.NoError(t, err, "failed to get row")
			require.Equal(t, fmt.Sprintf("EntityName%d", i+1), row[1], "unexpected ENTITY_NAME value")
			require.Equal(t, fmt.Sprintf("EntityKey%d", i+1), row[2], "unexpected ENTITY_KEY value")

			// Marshal EVENT_PAYLOAD to JSON string for comparison
			eventPayloadJSON, err := json.Marshal(row[3])
			require.NoError(t, err, "failed to marshal EVENT_PAYLOAD")
			require.JSONEq(t, fmt.Sprintf(`{"key%d": "value%d"}`, i+1, i+1), string(eventPayloadJSON), "unexpected EVENT_PAYLOAD value")
		}

		// Verify each row content using resultSet.GetRowScan(i)
		for i := 0; i < 10; i++ {
			rowScan, err := resultSet.GetRowScan(i)
			require.NoError(t, err, "failed to get row scan")

			var messageId int
			var entityName, entityKey string
			var eventPayload map[string]interface{}
			var modifyTs interface{}
			err = rowScan.Scan(&messageId, &entityName, &entityKey, &eventPayload, &modifyTs)
			require.NoError(t, err, "failed to scan row")
			require.Equal(t, fmt.Sprintf("EntityName%d", i+1), entityName, "unexpected ENTITY_NAME value")
			require.Equal(t, fmt.Sprintf("EntityKey%d", i+1), entityKey, "unexpected ENTITY_KEY value")

			// Marshal EVENT_PAYLOAD to JSON string for comparison
			eventPayloadJSON, err := json.Marshal(eventPayload)
			require.NoError(t, err, "failed to marshal EVENT_PAYLOAD")
			require.JSONEq(t, fmt.Sprintf(`{"key%d": "value%d"}`, i+1, i+1), string(eventPayloadJSON), "unexpected EVENT_PAYLOAD value")
		}
	})

	t.Run("TestQueryAndClose", func(t *testing.T) {
		defer cleanup()

		waitForDBReady(ctx, t, db)

		// Insert multiple rows into the EVENT_LOG table with varying fields
		insertQuery := `
			INSERT INTO EVENT_LOG (ENTITY_NAME, ENTITY_KEY, EVENT_PAYLOAD)
			VALUES ($1, $2, $3)
		`
		for i := 1; i <= 10; i++ {
			_, err := db.Exec(ctx, 0, insertQuery, fmt.Sprintf("EntityName%d", i), fmt.Sprintf("EntityKey%d", i), fmt.Sprintf(`{"key%d": "value%d"}`, i, i))
			require.NoError(t, err, "failed to insert row")
		}

		// Query the rows back from the EVENT_LOG table
		selectQuery := `
			SELECT MESSAGE_ID, ENTITY_NAME, ENTITY_KEY, EVENT_PAYLOAD, MODIFY_TS
			FROM EVENT_LOG
			ORDER BY MESSAGE_ID
		`
		resultSet, err := db.QueryAndClose(ctx, 0, selectQuery)
		require.NoError(t, err, "failed to query rows")

		// Ensure we have 10 rows in the resultSet
		require.Equal(t, 10, len(resultSet.GetRows()), "expected 10 rows in resultSet")
		require.Equal(t, 10, len(resultSet.GetRowsScan()), "expected 10 rows in RowsScan")

		// Verify each row content using resultSet.GetRow(i)
		for i := 0; i < 10; i++ {
			row, err := resultSet.GetRow(i)
			require.NoError(t, err, "failed to get row")
			require.Equal(t, fmt.Sprintf("EntityName%d", i+1), row[1], "unexpected ENTITY_NAME value")
			require.Equal(t, fmt.Sprintf("EntityKey%d", i+1), row[2], "unexpected ENTITY_KEY value")

			// Marshal EVENT_PAYLOAD to JSON string for comparison
			eventPayloadJSON, err := json.Marshal(row[3])
			require.NoError(t, err, "failed to marshal EVENT_PAYLOAD")
			require.JSONEq(t, fmt.Sprintf(`{"key%d": "value%d"}`, i+1, i+1), string(eventPayloadJSON), "unexpected EVENT_PAYLOAD value")
		}

		// Verify each row content using resultSet.GetRowScan(i)
		for i := 0; i < 10; i++ {
			rowScan, err := resultSet.GetRowScan(i)
			require.NoError(t, err, "failed to get row scan")

			var messageId int
			var entityName, entityKey string
			var eventPayload map[string]interface{}
			var modifyTs interface{}
			err = rowScan.Scan(&messageId, &entityName, &entityKey, &eventPayload, &modifyTs)
			require.NoError(t, err, "failed to scan row")
			require.Equal(t, fmt.Sprintf("EntityName%d", i+1), entityName, "unexpected ENTITY_NAME value")
			require.Equal(t, fmt.Sprintf("EntityKey%d", i+1), entityKey, "unexpected ENTITY_KEY value")

			// Marshal EVENT_PAYLOAD to JSON string for comparison
			eventPayloadJSON, err := json.Marshal(eventPayload)
			require.NoError(t, err, "failed to marshal EVENT_PAYLOAD")
			require.JSONEq(t, fmt.Sprintf(`{"key%d": "value%d"}`, i+1, i+1), string(eventPayloadJSON), "unexpected EVENT_PAYLOAD value")
		}
	})

	t.Run("TestTxQuery", func(t *testing.T) {
		defer cleanup()

		waitForDBReady(ctx, t, db)

		// Insert multiple rows into the EVENT_LOG table with varying fields
		insertQuery := `
			INSERT INTO EVENT_LOG (ENTITY_NAME, ENTITY_KEY, EVENT_PAYLOAD)
			VALUES ($1, $2, $3)
		`
		for i := 1; i <= 10; i++ {
			_, err := db.Exec(ctx, 0, insertQuery, fmt.Sprintf("EntityName%d", i), fmt.Sprintf("EntityKey%d", i), fmt.Sprintf(`{"key%d": "value%d"}`, i, i))
			require.NoError(t, err, "failed to insert row")
		}

		// Begin a transaction
		tx, err := db.TxBegin(ctx, 0)
		require.NoError(t, err, "failed to begin transaction")

		// Query the rows back from the EVENT_LOG table within the transaction
		selectQuery := `
			SELECT MESSAGE_ID, ENTITY_NAME, ENTITY_KEY, EVENT_PAYLOAD, MODIFY_TS
			FROM EVENT_LOG
			ORDER BY MESSAGE_ID
		`
		resultSet, err := tx.TxQuery(ctx, selectQuery)
		require.NoError(t, err, "failed to query rows")

		resultSet.Close()

		// Ensure we have 10 rows in the resultSet
		require.Equal(t, 10, len(resultSet.GetRows()), "expected 10 rows in resultSet")
		require.Equal(t, 10, len(resultSet.GetRowsScan()), "expected 10 rows in RowsScan")

		// Verify each row content using resultSet.GetRow(i)
		for i := 0; i < 10; i++ {
			row, err := resultSet.GetRow(i)
			require.NoError(t, err, "failed to get row")
			require.Equal(t, fmt.Sprintf("EntityName%d", i+1), row[1], "unexpected ENTITY_NAME value")
			require.Equal(t, fmt.Sprintf("EntityKey%d", i+1), row[2], "unexpected ENTITY_KEY value")

			// Marshal EVENT_PAYLOAD to JSON string for comparison
			eventPayloadJSON, err := json.Marshal(row[3])
			require.NoError(t, err, "failed to marshal EVENT_PAYLOAD")
			require.JSONEq(t, fmt.Sprintf(`{"key%d": "value%d"}`, i+1, i+1), string(eventPayloadJSON), "unexpected EVENT_PAYLOAD value")
		}

		// Verify each row content using resultSet.GetRowScan(i)
		for i := 0; i < 10; i++ {
			rowScan, err := resultSet.GetRowScan(i)
			require.NoError(t, err, "failed to get row scan")

			var messageId int
			var entityName, entityKey string
			var eventPayload []byte
			var modifyTs interface{}
			err = rowScan.Scan(&messageId, &entityName, &entityKey, &eventPayload, &modifyTs)
			require.NoError(t, err, "failed to scan row")
			require.Equal(t, fmt.Sprintf("EntityName%d", i+1), entityName, "unexpected ENTITY_NAME value")
			require.Equal(t, fmt.Sprintf("EntityKey%d", i+1), entityKey, "unexpected ENTITY_KEY value")

			// Marshal EVENT_PAYLOAD to JSON string for comparison
			require.JSONEq(t, fmt.Sprintf(`{"key%d": "value%d"}`, i+1, i+1), string(eventPayload), "unexpected EVENT_PAYLOAD value")
		}

		// Commit the transaction
		err = tx.TxCommit(ctx, 0)
		require.NoError(t, err, "failed to commit transaction")
	})

	t.Run("TestTxQueryAndClose", func(t *testing.T) {
		defer cleanup()

		waitForDBReady(ctx, t, db)

		// Insert multiple rows into the EVENT_LOG table with varying fields
		insertQuery := `
			INSERT INTO EVENT_LOG (ENTITY_NAME, ENTITY_KEY, EVENT_PAYLOAD)
			VALUES ($1, $2, $3)
		`
		for i := 1; i <= 10; i++ {
			_, err := db.Exec(ctx, 0, insertQuery, fmt.Sprintf("EntityName%d", i), fmt.Sprintf("EntityKey%d", i), fmt.Sprintf(`{"key%d": "value%d"}`, i, i))
			require.NoError(t, err, "failed to insert row")
		}

		// Begin a transaction
		tx, err := db.TxBegin(ctx, 0)
		require.NoError(t, err, "failed to begin transaction")

		// Query the rows back from the EVENT_LOG table within the transaction
		selectQuery := `
			SELECT MESSAGE_ID, ENTITY_NAME, ENTITY_KEY, EVENT_PAYLOAD, MODIFY_TS
			FROM EVENT_LOG
			ORDER BY MESSAGE_ID
		`
		resultSet, err := tx.TxQueryAndClose(ctx, selectQuery)
		require.NoError(t, err, "failed to query rows")

		// Ensure we have 10 rows in the resultSet
		require.Equal(t, 10, len(resultSet.GetRows()), "expected 10 rows in resultSet")
		require.Equal(t, 10, len(resultSet.GetRowsScan()), "expected 10 rows in RowsScan")

		// Verify each row content using resultSet.GetRow(i)
		for i := 0; i < 10; i++ {
			row, err := resultSet.GetRow(i)
			require.NoError(t, err, "failed to get row")
			require.Equal(t, fmt.Sprintf("EntityName%d", i+1), row[1], "unexpected ENTITY_NAME value")
			require.Equal(t, fmt.Sprintf("EntityKey%d", i+1), row[2], "unexpected ENTITY_KEY value")

			// Marshal EVENT_PAYLOAD to JSON string for comparison
			eventPayloadJSON, err := json.Marshal(row[3])
			require.NoError(t, err, "failed to marshal EVENT_PAYLOAD")
			require.JSONEq(t, fmt.Sprintf(`{"key%d": "value%d"}`, i+1, i+1), string(eventPayloadJSON), "unexpected EVENT_PAYLOAD value")
		}

		// Verify each row content using resultSet.GetRowScan(i)
		for i := 0; i < 10; i++ {
			rowScan, err := resultSet.GetRowScan(i)
			require.NoError(t, err, "failed to get row scan")

			var messageId int
			var entityName, entityKey string
			var eventPayload []byte
			var modifyTs interface{}
			err = rowScan.Scan(&messageId, &entityName, &entityKey, &eventPayload, &modifyTs)
			require.NoError(t, err, "failed to scan row")
			require.Equal(t, fmt.Sprintf("EntityName%d", i+1), entityName, "unexpected ENTITY_NAME value")
			require.Equal(t, fmt.Sprintf("EntityKey%d", i+1), entityKey, "unexpected ENTITY_KEY value")

			// Marshal EVENT_PAYLOAD to JSON string for comparison
			require.JSONEq(t, fmt.Sprintf(`{"key%d": "value%d"}`, i+1, i+1), string(eventPayload), "unexpected EVENT_PAYLOAD value")
		}

		// Commit the transaction
		err = tx.TxCommit(ctx, 0)
		require.NoError(t, err, "failed to commit transaction")
	})

	t.Run("TestQueryAndProcess", func(t *testing.T) {
		defer cleanup()

		waitForDBReady(ctx, t, db)

		// Insert multiple rows into the EVENT_LOG table with varying fields
		insertQuery := `
			INSERT INTO EVENT_LOG (ENTITY_NAME, ENTITY_KEY, EVENT_PAYLOAD)
			VALUES ($1, $2, $3)
		`
		for i := 1; i <= 10; i++ {
			_, err := db.Exec(ctx, 0, insertQuery, fmt.Sprintf("EntityName%d", i), fmt.Sprintf("EntityKey%d", i), fmt.Sprintf(`{"key%d": "value%d"}`, i, i))
			require.NoError(t, err, "failed to insert row")
		}

		// Query the rows back from the EVENT_LOG table using QueryAndProcess
		selectQuery := `
			SELECT MESSAGE_ID, ENTITY_NAME, ENTITY_KEY, EVENT_PAYLOAD, MODIFY_TS
			FROM EVENT_LOG
			ORDER BY MESSAGE_ID
		`
		var rowsProcessed int
		err := db.QueryAndProcess(ctx, 0, func(row database.Row, rowScan database.RowScan) error {
			// Process each row and verify its content
			var messageId int
			var entityName, entityKey string
			var eventPayload map[string]interface{}
			var modifyTs interface{}
			err := rowScan.Scan(&messageId, &entityName, &entityKey, &eventPayload, &modifyTs)
			require.NoError(t, err, "failed to scan row")
			require.Equal(t, fmt.Sprintf("EntityName%d", rowsProcessed+1), entityName, "unexpected ENTITY_NAME value")
			require.Equal(t, fmt.Sprintf("EntityKey%d", rowsProcessed+1), entityKey, "unexpected ENTITY_KEY value")

			// Marshal EVENT_PAYLOAD to JSON string for comparison
			eventPayloadJSON, err := json.Marshal(eventPayload)
			require.NoError(t, err, "failed to marshal EVENT_PAYLOAD")
			require.JSONEq(t, fmt.Sprintf(`{"key%d": "value%d"}`, rowsProcessed+1, rowsProcessed+1), string(eventPayloadJSON), "unexpected EVENT_PAYLOAD value")

			rowsProcessed++
			return nil
		}, selectQuery)
		require.NoError(t, err, "failed to query and process rows")

		// Ensure we processed 10 rows
		require.Equal(t, 10, rowsProcessed, "expected to process 10 rows")
	})
}
