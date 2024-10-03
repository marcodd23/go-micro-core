package dbx

import (
	"context"
)

// InstanceManager defines a contract for managing database connections and executing queries within a database instance.
//
// This interface abstracts the operations related to managing a database connection pool, initiating transactions,
// and executing SQL queries. It provides an API for interacting with a database instance, regardless of the
// specific database implementation being used.
//
// Responsibilities of InstanceManager include:
//   - Managing the lifecycle of the database connection pool, including acquiring and releasing connections.
//   - Initiating transactions and returning a Transaction interface for executing queries within that transaction context.
//   - Executing SQL queries and commands directly, outside of a transaction context, with support for handling distributed locks if needed.
//   - Providing access to the connection configuration.
//
// The methods within this interface are designed to be flexible and allow for different database drivers or implementations
// to be used under a common API, making it easier to switch or extend database functionality without altering application logic.
//
// Example Implementation:
//
//	A concrete implementation of InstanceManager might use a specific database driver like pgx for PostgreSQL, managing
//	connections, transactions, and queries in a PostgreSQL-compatible manner while adhering to the interface contract.
type InstanceManager interface {
	GetDbConnPool() (any, error)
	GetConnFromPool(ctx context.Context) (any, error)
	CloseDbConnPool()
	GetConnectionConfig() ConnConfig
	TxBegin(ctx context.Context) (Transaction, error)
	Query(ctx context.Context, distLockId int64, query string, args ...interface{}) (conn any, rows any, err error)
	Exec(ctx context.Context, distLockId int64, execQuery string, args ...any) (int64, error)
}
