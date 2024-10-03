package dbx

// =====================================
// Batch Interface
// =====================================

// Batch defines the interface for managing a batch of SQL statements to be executed within a transaction.
//
// A Batch allows multiple SQL statements to be queued together and executed in a single batch operation,
// which can improve performance by reducing the number of round trips to the database. This interface
// abstracts the batch management
//
// Methods:
//
//   - GetBatch: Returns the underlying batch object, allowing for direct interaction with the batch if needed.
//   - Len: Returns the number of SQL statements currently queued in the batch.
//   - Queue: Adds a SQL statement to the batch with the provided query and arguments.
//
// Example Usage:
//
//	var batch dbx.Batch = NewEmptyBatch()
//	batch.Queue("INSERT INTO users (name, email) VALUES ($1, $2)", "John Doe", "john@example.com")
//	batch.Queue("UPDATE users SET last_login = now() WHERE id = $1", userID)
//
//	rowsAffected, err := ExecuteBatch(ctx, batch)
//	if err != nil {
//	    log.Fatal("Failed to execute batch:", err)
//	}
//	log.Printf("Batch executed successfully, rows affected: %d", rowsAffected)
type Batch interface {
	GetBatch() any
	Len() int
	Queue(query string, arguments ...any)
}
