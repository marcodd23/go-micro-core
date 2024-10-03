package pgxdb

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/marcodd23/go-micro-core/pkg/dbx"
	"github.com/pkg/errors"
)

// QueryAndScan executes a query and maps the result to structs using the provided scanFunc.
//
// This function simplifies the process of executing a SQL query and converting each row in the result set
// to a specific struct type using a custom scan function. It handles the query execution, iteration over
// the rows, and error management.
//
// Arguments:
//   - mgr: The instance manager responsible for managing the database connection.
//   - ctx: The context for the query execution, which can be used to control cancellation and deadlines.
//   - distLockId: The ID used to retrieve a specific connection associated with a distributed lock. If 0,
//     it gets a connection from the general pool.
//   - scanFunc: A function that maps each row (pgx.Rows) to the desired struct type (T).
//   - query: The SQL query to be executed.
//   - args: The variadic arguments for the SQL query, if any.
//
// Returns:
//   - []T: A slice of the struct type T, representing the mapped results from the query.
//   - error: Any error encountered during query execution or row scanning.
func QueryAndScan[T any](mgr dbx.InstanceManager, ctx context.Context, distLockId int64, scanFunc func(rows pgx.Rows) (T, error), query string, args ...interface{}) ([]T, error) {
	conn, rows, err := mgr.Query(ctx, distLockId, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	pgxRows := rows.(pgx.Rows)
	pgxConn := conn.(*pgxpool.Conn)

	defer func() {
		pgxRows.Close()
		pgxConn.Release()
	}()

	var results []T
	for pgxRows.Next() {
		result, err := scanFunc(pgxRows)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		results = append(results, result)
	}

	if err := pgxRows.Err(); err != nil {
		return nil, errors.WithStack(err)
	}

	return results, nil
}

// QueryScanAndProcess executes a query and processes each row with a callback that receives a struct.
//
// This function is designed to execute a SQL query and process each resulting row as a struct of type T.
// The rows are mapped to the struct using a custom scan function provided by the caller. Each struct is
// then passed to a callback function for processing, allowing for side effects or further actions to be
// performed on each individual result.
//
// Arguments:
//   - mgr: The instance manager responsible for managing the database connection.
//   - ctx: The context for the query execution, allowing for cancellation and timeout management.
//   - distLockId: The ID used to retrieve a specific connection associated with a distributed lock. If 0,
//     it gets a connection from the general pool.
//   - query: The SQL query to be executed.
//   - scanFunc: A function that maps each row (pgx.Rows) to the desired struct type (T).
//   - processCallbackFunc: A callback function that processes each mapped struct (T).
//   - args: The variadic arguments for the SQL query, if any.
//
// Returns:
//   - error: Any error encountered during query execution, row scanning, or processing.
func QueryScanAndProcess[T any](mgr dbx.InstanceManager, ctx context.Context, distLockId int64, query string, scanFunc func(rows pgx.Rows) (T, error), processCallbackFunc func(item T) error, args ...interface{}) error {
	conn, rows, err := mgr.Query(ctx, distLockId, query, args...)
	if err != nil {
		return errors.WithStack(err)
	}

	pgxRows := rows.(pgx.Rows)
	pgxConn := conn.(*pgxpool.Conn)

	defer func() {
		pgxRows.Close()
		pgxConn.Release()
	}()

	for pgxRows.Next() {
		item, err := scanFunc(rows.(pgx.Rows))
		if err != nil {
			return errors.WithStack(err)
		}
		if err := processCallbackFunc(item); err != nil {
			return errors.WithStack(err)
		}
	}
	return pgxRows.Err()
}

// QueryAndMap uses pgx's struct scanning to map rows directly to a slice of structs.
//
// This function leverages pgx's built-in support for struct scanning to execute a query and automatically
// map each row in the result set to a struct of type T. The function eliminates the need for a custom
// scan function, making it easier to work with structured data.
//
// Arguments:
//   - mgr: The instance manager responsible for managing the database connection.
//   - ctx: The context for the query execution, which can manage cancellation and deadlines.
//   - distLockId: The ID used to retrieve a specific connection associated with a distributed lock. If 0,
//     it gets a connection from the general pool.
//   - query: The SQL query to be executed.
//   - args: The variadic arguments for the SQL query, if any.
//
// Returns:
//   - []T: A slice of the struct type T, representing the mapped results from the query.
//   - error: Any error encountered during query execution or row mapping.
func QueryAndMap[T any](mgr dbx.InstanceManager, ctx context.Context, distLockId int64, query string, args ...interface{}) ([]T, error) {
	conn, rows, err := mgr.Query(ctx, distLockId, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	pgxRows := rows.(pgx.Rows)
	pgxConn := conn.(*pgxpool.Conn)

	defer func() {
		pgxRows.Close()
		pgxConn.Release()
	}()

	results, err := pgx.CollectRows(pgxRows, pgx.RowToStructByName[T])
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return results, nil
}

// QueryMapAndProcess uses pgx's struct scanning to map rows directly to a struct and then process each struct.
//
// This function combines pgx's struct scanning feature with a processing callback. It executes a query,
// maps each resulting row to a struct of type T using pgx's built-in capabilities, and then passes each
// struct to a callback function for further processing.
//
// Arguments:
//   - mgr: The instance manager responsible for managing the database connection.
//   - ctx: The context for the query execution, which can manage cancellation and deadlines.
//   - distLockId: The ID used to retrieve a specific connection associated with a distributed lock. If 0,
//     it gets a connection from the general pool.
//   - query: The SQL query to be executed.
//   - processCallbackFunc: A callback function that processes each mapped struct (T).
//   - args: The variadic arguments for the SQL query, if any.
//
// Returns:
//   - error: Any error encountered during query execution, row mapping, or processing.
func QueryMapAndProcess[T any](mgr dbx.InstanceManager, ctx context.Context, distLockId int64, query string, processCallbackFunc func(item T) error, args ...interface{}) error {
	conn, rows, err := mgr.Query(ctx, distLockId, query, args...)
	if err != nil {
		return errors.WithStack(err)
	}

	pgxRows := rows.(pgx.Rows)
	pgxConn := conn.(*pgxpool.Conn)

	defer func() {
		pgxRows.Close()
		pgxConn.Release()
	}()

	for pgxRows.Next() {
		item, err := pgx.RowToStructByName[T](pgxRows)
		if err != nil {
			return errors.WithStack(err)
		}
		if err := processCallbackFunc(item); err != nil {
			return errors.WithStack(err)
		}
	}

	return pgxRows.Err()
}
