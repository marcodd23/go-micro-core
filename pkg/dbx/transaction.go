package dbx

import (
	"context"
)

// Transaction defines the interface for managing database transactions.
//
// This interface abstracts the operations required to manage a database transaction, providing methods for committing,
// rolling back, and executing queries within the transaction context. It allows for consistent transaction management
// across different database implementations, ensuring that operations can be performed atomically and reliably.
//
// Responsibilities of the Transaction interface include:
//   - Providing access to the underlying transaction object, allowing for direct interaction if necessary.
//   - Committing the transaction to persist all changes made within the transaction.
//   - Rolling back the transaction to discard all changes made within the transaction.
//   - Executing SQL queries and commands within the transaction context, ensuring that they are part of the atomic operation.
//
// The methods in this interface are designed to be flexible and support different database drivers or implementations,
// allowing for consistent transaction management across various database systems.
//
// Example Implementation:
//
//	A concrete implementation of Transaction might use a specific database driver like pgx for PostgreSQL, managing
//	transaction operations in a PostgreSQL-compatible manner while adhering to the interface contract.
type Transaction interface {
	GetTx() any
	TxCommit(ctx context.Context) error
	TxRollback(ctx context.Context)
	TxQuery(ctx context.Context, query string, args ...interface{}) (any, error)
	TxExec(ctx context.Context, query string, args ...any) (int64, error)
}
