package pgxdb

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/marcodd23/go-micro-core/pkg/dbx"
	"github.com/pkg/errors"
)

// TxQueryAndScan executes a query within a transaction and maps the result to structs using the provided scanFunc.
//
// This function simplifies the process of executing a SQL query within a transaction and converting each row
// in the result set to a specific struct type using a custom scan function. It handles the query execution,
// iteration over the rows, and error management.
//
// Arguments:
//   - tx: The transaction object within which the query is executed.
//   - ctx: The context for the query execution, which can be used to control cancellation and deadlines.
//   - scanFunc: A function that maps each row (pgx.Rows) to the desired struct type (T).
//   - query: The SQL query to be executed.
//   - args: The variadic arguments for the SQL query, if any.
//
// Returns:
//   - []T: A slice of the struct type T, representing the mapped results from the query.
//   - error: Any error encountered during query execution or row scanning.
func TxQueryAndScan[T any](tx dbx.Transaction, ctx context.Context, scanFunc func(rows pgx.Rows) (T, error), query string, args ...interface{}) ([]T, error) {
	rows, err := tx.TxQuery(ctx, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	defer rows.(pgx.Rows).Close()

	var results []T
	for rows.(pgx.Rows).Next() {
		result, err := scanFunc(rows.(pgx.Rows))
		if err != nil {
			return nil, errors.Wrap(err, "TxQueryAndScan error scanFunc")
		}
		results = append(results, result)
	}

	if err := rows.(pgx.Rows).Err(); err != nil {
		return nil, errors.Wrap(err, "TxQueryAndScan error mapping rows with scanFunc")
	}

	return results, nil
}

// TxQueryScanAndProcess executes a query within a transaction and processes each row with a callback that receives a struct.
//
// This function is designed to execute a SQL query within a transaction and process each resulting row as a struct
// of type T. The rows are mapped to the struct using a custom scan function provided by the caller. Each struct is
// then passed to a callback function for processing, allowing for side effects or further actions to be performed
// on each individual result.
//
// Arguments:
//   - tx: The transaction object within which the query is executed.
//   - ctx: The context for the query execution, allowing for cancellation and timeout management.
//   - query: The SQL query to be executed.
//   - scanFunc: A function that maps each row (pgx.Rows) to the desired struct type (T).
//   - processCallbackFunc: A callback function that processes each mapped struct (T).
//   - args: The variadic arguments for the SQL query, if any.
//
// Returns:
//   - error: Any error encountered during query execution, row scanning, or processing.
func TxQueryScanAndProcess[T any](tx dbx.Transaction, ctx context.Context, query string, scanFunc func(rows pgx.Rows) (T, error), processCallbackFunc func(item T) error, args ...interface{}) error {
	rows, err := tx.TxQuery(ctx, query, args...)
	if err != nil {
		return errors.WithStack(err)
	}

	defer rows.(pgx.Rows).Close()

	for rows.(pgx.Rows).Next() {
		item, err := scanFunc(rows.(pgx.Rows))
		if err != nil {
			return errors.Wrap(err, "TxQueryScanAndProcess error mapping rows with scanFunc")
		}

		if err := processCallbackFunc(item); err != nil {
			return errors.Wrap(err, "TxQueryScanAndProcess error executing processCallbackFunc")
		}
	}

	return rows.(pgx.Rows).Err()
}

// TxQueryAndMap uses pgx's struct scanning to map rows directly to a slice of structs within a transaction.
//
// This function leverages pgx's built-in support for struct scanning to execute a query within a transaction
// and automatically map each row in the result set to a struct of type T. The function eliminates the need
// for a custom scan function, making it easier to work with structured data.
//
// Arguments:
//   - tx: The transaction object within which the query is executed.
//   - ctx: The context for the query execution, which can manage cancellation and deadlines.
//   - query: The SQL query to be executed.
//   - args: The variadic arguments for the SQL query, if any.
//
// Returns:
//   - []T: A slice of the struct type T, representing the mapped results from the query.
//   - error: Any error encountered during query execution or row mapping.
func TxQueryAndMap[T any](tx dbx.Transaction, ctx context.Context, query string, args ...interface{}) ([]T, error) {
	rows, err := tx.TxQuery(ctx, query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	defer rows.(pgx.Rows).Close()

	results, err := pgx.CollectRows(rows.(pgx.Rows), pgx.RowToStructByName[T])
	if err != nil {
		return nil, errors.Wrap(err, "TxQueryAndMap error mapping and collecting rows to struct slice")
	}

	return results, nil
}

// TxQueryMapAndProcess uses pgx's struct scanning to map rows directly to a struct and then process each struct within a transaction.
//
// This function combines pgx's struct scanning feature with a processing callback. It executes a query within
// a transaction, maps each resulting row to a struct of type T using pgx's built-in capabilities, and then
// passes each struct to a callback function for further processing.
//
// Arguments:
//   - tx: The transaction object within which the query is executed.
//   - ctx: The context for the query execution, which can manage cancellation and deadlines.
//   - query: The SQL query to be executed.
//   - processCallbackFunc: A callback function that processes each mapped struct (T).
//   - args: The variadic arguments for the SQL query, if any.
//
// Returns:
//   - error: Any error encountered during query execution, row mapping, or processing.
func TxQueryMapAndProcess[T any](tx dbx.Transaction, ctx context.Context, query string, processCallbackFunc func(item T) error, args ...interface{}) error {
	rows, err := tx.TxQuery(ctx, query, args...)
	if err != nil {
		return errors.WithStack(err)
	}

	defer rows.(pgx.Rows).Close()

	for rows.(pgx.Rows).Next() {
		item, err := pgx.RowToStructByName[T](rows.(pgx.Rows))
		if err != nil {
			return errors.Wrap(err, "TxQueryMapAndProcess error mapping row to struct")
		}

		if err := processCallbackFunc(item); err != nil {
			return errors.Wrap(err, "TxQueryMapAndProcess error executing processCallbackFunc")
		}
	}
	return rows.(pgx.Rows).Err()
}

// TxExecBatch processes the results of a batch of SQL statements within a single transaction and returns the total number of rows affected.
//
// This function sends a batch of SQL statements for execution within the context of an existing transaction. After the batch
// is sent, it processes the results of each query in the batch sequentially. If any query in the batch fails, the function returns
// an error and stops further processing.
//
// Arguments:
//   - tx: The transaction within which the batch will be processed. This must implement the dbx.Transaction interface.
//   - ctx: The context for managing the batch processing, allowing for cancellation and timeouts.
//   - batch: The batch of SQL statements to be processed. This must implement the dbx.Batch interface.
//
// Returns:
//   - int64: The total number of rows affected by all the SQL statements in the batch.
//   - error: An error if any query in the batch fails, otherwise nil.
//
// Behavior:
//   - The function first converts the custom dbx.Batch interface to the underlying pgx.Batch.
//   - It then sends the batch for execution within the transaction context using the pgx.SendBatch method.
//   - The function iterates over each query result in the batch, reading the outcome and accumulating the total number of rows affected.
//   - If reading the result of any query fails, the function captures the error, stops further processing, and returns the error along with the number of rows affected up to that point.
//
// Example Usage:
//
//	batch := pgxdb.NewEmptyBatch()
//	batch.Queue("INSERT INTO users (name, email) VALUES ($1, $2)", "John Doe", "john@example.com")
//	batch.Queue("UPDATE users SET last_login = now() WHERE id = $1", userID)
//
//	rowsAffected, err := pgxdb.TxExecBatch(tx, ctx, batch)
//	if err != nil {
//	    log.Fatal("Failed to execute batch:", err)
//	}
//	log.Printf("Batch executed successfully, total rows affected: %d", rowsAffected)
func TxExecBatch(tx dbx.Transaction, ctx context.Context, batch dbx.Batch) (int64, error) {
	// Convert the custom batch interface to the underlying pgx.Batch.
	pgxBatch := batch.GetBatch().(*pgx.Batch)

	// Send the batch to be executed in the context of the current transaction.
	batchResult := tx.GetTx().(pgx.Tx).SendBatch(ctx, pgxBatch)
	defer batchResult.Close()

	var totalRowsAffected int64
	var batchErr error

	for i := 0; i < pgxBatch.Len(); i++ {
		ct, err := batchResult.Exec()
		if err != nil {
			batchErr = errors.Wrap(err, "batch execution failed")
			return totalRowsAffected, batchErr
		}

		totalRowsAffected += ct.RowsAffected()
	}

	// Return the total number of rows affected across all batch queries.
	return totalRowsAffected, batchErr
}

// ExecTransactionalTask performs a task inside a transaction.
//
// This function facilitates the execution of a series of database operations within a transaction. It
// ensures that all operations either succeed or fail together, maintaining the integrity of the data. If
// any error occurs during the task, the transaction is rolled back. Otherwise, the transaction is committed.
//
// Arguments:
//   - mgr: The instance manager responsible for managing the database connection.
//   - ctx: The context for the transaction execution, which can manage timeouts and cancellation.
//   - distLockId: The ID used to retrieve a specific connection associated with a distributed lock. If 0,
//     it gets a connection from the general pool.
//   - task: A function that encapsulates the operations to be performed within the transaction. This function
//     receives the context and the transaction object, allowing it to execute queries within the transaction.
//   - args: Any additional arguments needed for the transaction execution.
//
// Returns:
//   - error: Any error encountered during transaction initiation, execution, or commit.
func ExecTransactionalTask(mgr dbx.InstanceManager, ctx context.Context, distLockId int64, task func(ctx context.Context, tx dbx.Transaction) error, args ...interface{}) error {
	pgTx, err := mgr.TxBegin(ctx)
	if err != nil {
		return errors.Wrap(err, "error starting transaction")
	}

	defer func() {
		if p := recover(); p != nil {
			pgTx.TxRollback(ctx)
			panic(p)
		} else if err != nil {
			pgTx.TxRollback(ctx)
		}
	}()

	err = task(ctx, pgTx)
	if err != nil {
		pgTx.TxRollback(ctx)
		return errors.Wrap(err, "error executing transactional task")
	}

	err = pgTx.TxCommit(ctx)
	if err != nil {
		return errors.Wrap(err, "error committing transaction")
	}

	return nil
}
