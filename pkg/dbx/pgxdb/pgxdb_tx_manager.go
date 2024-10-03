package pgxdb

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/marcodd23/go-micro-core/pkg/errorx"
	"github.com/marcodd23/go-micro-core/pkg/logx"
)

//###################################
//#       Postgres TX Manager       #
//###################################

// PostgresTx - Postgres Transaction manager.
// Implements dbx.Transaction, providing methods to manage a PostgreSQL transaction.
type PostgresTx struct {
	tx   pgx.Tx
	conn *pgxpool.Conn
	txId int64
}

// GetTx - Returns the underlying pgx transaction.
//
// This method exposes the underlying pgx.Tx object, allowing for direct interaction with
// the transaction if needed. It is primarily used internally to execute queries or commands
// within the transaction's context.
//
// Returns:
//   - any: The underlying pgx.Tx object.
func (tx *PostgresTx) GetTx() any {
	return tx.tx
}

// TxCommit - Commits a transaction and releases the connection to the pool.
//
// This method finalizes the transaction by committing all changes made during the transaction.
// After the commit, the database connection is released back to the connection pool for reuse.
//
// Arguments:
//   - ctx: The context for managing the transaction commit, which allows for cancellation and timeouts.
//
// Returns:
//   - error: Any error encountered during the commit process. If the commit fails, the error is wrapped in a custom error type.
//
// Example Usage:
//
//	err := tx.TxCommit(ctx)
//	if err != nil {
//	    log.Fatal("Failed to commit transaction:", err)
//	}
func (tx *PostgresTx) TxCommit(ctx context.Context) error {
	defer tx.conn.Release()
	err := tx.tx.Commit(ctx)
	if err != nil {
		logx.GetLogger().LogError(ctx, "error during transaction commit", err)
		return errorx.NewDatabaseErrorWrapper(err, "error during transaction commit")
	}

	return nil
}

// TxRollback - Rolls back a transaction and releases the connection to the pool.
//
// This method aborts the transaction, discarding all changes made during the transaction.
// After the rollback, the database connection is released back to the connection pool.
//
// Arguments:
//   - ctx: The context for managing the transaction rollback, which allows for cancellation and timeouts.
//
// Example Usage:
//
//	tx.TxRollback(ctx) // No error returned, typically used in a deferred call
func (tx *PostgresTx) TxRollback(ctx context.Context) {
	err := tx.tx.Rollback(ctx)
	if err != nil {
		logx.GetLogger().LogError(ctx, fmt.Sprintf("error Rolling Back transaction: %d", tx.txId), err)
	} else {
		logx.GetLogger().LogDebug(ctx, fmt.Sprintf("Rollack transaction: %d", tx.txId))
	}
}

// TxQuery executes a query within the transaction and returns pgx.Rows.
//
// This method is used to execute a SQL query that returns rows, such as a SELECT statement,
// within the context of the active transaction. It returns a pgx.Rows object that can be used
// to iterate over the results.
//
// Arguments:
//   - ctx: The context for managing the query execution, which allows for cancellation and timeouts.
//   - query: The SQL query to be executed.
//   - args: The arguments for the SQL query.
//
// Returns:
//   - any: The resulting rows from the query, typically of type pgx.Rows.
//   - error: Any error encountered during the query execution.
//
// Example Usage:
//
//	rows, err := tx.TxQuery(ctx, "SELECT * FROM users WHERE id = $1", userID)
//	if err != nil {
//	    log.Fatal("Failed to execute query:", err)
//	}
//	defer rows.Close()
func (tx *PostgresTx) TxQuery(ctx context.Context, query string, args ...interface{}) (any, error) {
	rows, err := tx.tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

// TxExec - Executes a command query under a transaction and returns the number of rows affected.
//
// This method is used to execute SQL commands that modify the database, such as INSERT, UPDATE, or DELETE,
// within the context of the active transaction. It returns the number of rows affected by the query.
//
// Arguments:
//   - ctx: The context for managing the query execution, which allows for cancellation and timeouts.
//   - execQuery: The SQL query to be executed.
//   - args: The arguments for the SQL query.
//
// Returns:
//   - int64: The number of rows affected by the query.
//   - error: Any error encountered during the query execution. If an error occurs, it is wrapped in a custom error type.
//
// Example Usage:
//
//	rowsAffected, err := tx.TxExec(ctx, "UPDATE users SET last_login = now() WHERE id = $1", userID)
//	if err != nil {
//	    log.Fatal("Failed to execute update:", err)
//	}
//	log.Printf("%d rows were updated", rowsAffected)
func (tx *PostgresTx) TxExec(ctx context.Context, execQuery string, args ...any) (int64, error) {
	result, err := tx.tx.Exec(ctx, execQuery, args...)
	if err != nil {
		return 0, errorx.NewDatabaseErrorWrapper(err, "Error executing query '%s'", execQuery)
	}

	return result.RowsAffected(), nil
}
