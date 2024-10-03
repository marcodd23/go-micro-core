package pgxdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/marcodd23/go-micro-core/pkg/dbx"
	"github.com/pkg/errors"
)

// BulkInsertEntitiesWithTags inserts a large number of structs into a PostgreSQL table using pgx.CopyFrom.
//
// This function takes a slice of structs that implement the `RowConvertibleEntity` interface, derives the column names from the
// `db` struct tags of the first struct, and inserts the data into the database. Each struct must implement the `ToRow()` method,
// which converts the struct to a row of values corresponding to the derived column names.
//
// The `db` tags in the struct define how the fields are mapped to the corresponding columns in the database. Fields with `db:"-"`
// or without a `db` tag will be ignored during the insertion process.
//
// Arguments:
//   - mgr: The instance manager responsible for managing the database connection.
//   - ctx: The context for the query execution, which can be used to control cancellation and deadlines.
//   - distLockId: The ID used to retrieve a specific connection associated with a distributed lock. If 0,
//     it gets a connection from the general pool.
//   - tableName: The name of the table into which data will be inserted (CASE SENSITIVE).
//   - entities: A slice of structs implementing the `RowConvertibleEntity` interface, representing the rows to be inserted.
//
// Returns:
//   - int64: The number of rows successfully inserted.
//   - error: Any error encountered during the bulk insert.
//
// Notes:
//   - The function derives column names from the first entity's `db` tags using the `DeriveColumnNamesFromTags` function.
//   - Each struct passed to the function must implement the `RowConvertibleEntity` interface, specifically the `ToRow()` method, which
//     returns the corresponding row values as a slice of `interface{}`.
//   - The `db` tag in the struct is used to map fields to PostgreSQL columns. Fields without a `db` tag or with `db:"-"` are skipped.
//   - The PostgreSQL `pgx.CopyFrom` function is used for efficient bulk insertion.
func BulkInsertEntitiesWithTags[T dbx.RowConvertibleEntity](mgr dbx.InstanceManager, ctx context.Context, distLockId int64, tableName string, entities []T) (int64, error) {
	// Check if there are any entities to insert
	if len(entities) == 0 {
		return 0, errors.New("no entities to insert")
	}

	// Derive the column names from the first entity's struct tags
	columnNames, err := dbx.DeriveColumnNamesFromTags(entities[0], "db")
	if err != nil {
		return 0, errors.Wrap(err, "error deriving column names")
	}

	// Get a connection from the instance manager
	conn, err := mgr.GetConnFromPool(ctx)
	if err != nil {
		return 0, err
	}
	pgxConn := conn.(*pgxpool.Conn)

	// Defer connection release
	defer pgxConn.Release()

	// Convert structs to [][]interface{} for pgx.CopyFromRows
	rows := make([][]interface{}, len(entities))
	for i, entity := range entities {
		rows[i] = entity.ToRow()
	}

	// Perform the bulk insert using pgx.CopyFrom
	rowCount, err := pgxConn.CopyFrom(
		ctx,
		splitTableName(tableName), // Table name
		columnNames,               // Column names derived from struct tags
		pgx.CopyFromRows(rows),    // The data source
	)

	if err != nil {
		return 0, errors.Wrap(err, "bulk insert error")
	}

	return rowCount, nil
}

func splitTableName(tableName string) pgx.Identifier {
	parts := strings.Split(tableName, ".")
	if len(parts) == 1 {
		// Only the table name is provided, assume the default schema
		return pgx.Identifier{parts[0]}
	} else if len(parts) == 2 {
		// Schema and table are provided
		return pgx.Identifier{parts[0], parts[1]}
	} else {
		// Handle error case for an invalid identifier
		panic(fmt.Sprintf("Invalid table name format: %s", tableName))
	}
}
