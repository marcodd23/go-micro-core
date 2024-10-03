package pgxdb

import (
	"github.com/jackc/pgx/v5"
	"github.com/marcodd23/go-micro-core/pkg/dbx"
)

//###################################
//#       Postgres BATCH            #
//###################################

// pgxBatch - Postgres Transaction Batch manager.
// Implements dbx.Batch, providing methods to manage a batch of SQL statements within a transaction.
type pgxBatch struct {
	batch *pgx.Batch
}

// NewEmptyBatch creates a new, empty batch for queuing SQL statements.
//
// This function returns an implementation of the dbx.Batch interface, which can be used to
// queue multiple SQL statements that will be executed in a single batch operation within a transaction.
//
// Returns:
//   - dbx.Batch: A new, empty batch ready to have SQL statements queued.
func NewEmptyBatch() dbx.Batch {
	return &pgxBatch{
		batch: &pgx.Batch{},
	}
}

// GetBatch returns the underlying pgx.Batch.
//
// This method exposes the underlying pgx.Batch object, allowing for direct interaction with the batch if needed.
// It ensures that the batch is initialized before returning it.
//
// Returns:
//   - any: The underlying pgx.Batch object.
func (bch *pgxBatch) GetBatch() any {
	if bch == nil {
		bch.batch = &pgx.Batch{}
	}

	return bch.batch
}

// Len returns the number of SQL statements queued in the batch.
//
// This method returns the count of SQL statements currently queued in the batch. It ensures that the batch
// is initialized before accessing the length.
//
// Returns:
//   - int: The number of SQL statements queued in the batch.
func (bch *pgxBatch) Len() int {
	if bch == nil {
		bch.batch = &pgx.Batch{}
	}

	return bch.batch.Len()
}

// Queue adds a SQL statement to the batch with the given query and arguments.
//
// This method queues a SQL statement into the batch to be executed later as part of a batch operation.
// It ensures that the batch is initialized before adding the query.
//
// Arguments:
//   - query: The SQL query to be queued.
//   - arguments: The arguments for the SQL query, which will be used in place of placeholders in the query.
//
// Example Usage:
//
//	batch.Queue("INSERT INTO users (name, email) VALUES ($1, $2)", "John Doe", "john@example.com")
func (bch *pgxBatch) Queue(query string, arguments ...any) {
	if bch == nil {
		bch.batch = &pgx.Batch{}
	}

	bch.batch.Queue(query, arguments...)
}
