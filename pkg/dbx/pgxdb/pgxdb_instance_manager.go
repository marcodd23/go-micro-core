package pgxdb

import (
	"context"
	"fmt"
	"runtime"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/marcodd23/go-micro-core/pkg/dbx"
	"github.com/marcodd23/go-micro-core/pkg/errorx"
	"github.com/marcodd23/go-micro-core/pkg/logx"
	"github.com/pkg/errors"
)

var (
	dbInstance dbx.InstanceManager
)

//###################################
//#    PostgresDB - dbx manager.     #
//###################################

// PostgresDB - dbx manager.
// It Implements dbx.InstanceManager
type PostgresDB struct {
	pool   *pgxpool.Pool
	dbConf dbx.ConnConfig
}

// SetupPostgresDbManager - setup Postgres DB connection.
func SetupPostgresDbManager(ctx context.Context, dbConf dbx.ConnConfig, preparesStatements ...dbx.PreparedStatement) dbx.InstanceManager {

	pool, err := newConnectionPool(ctx, dbConf, preparesStatements...)
	if err != nil {
		logx.GetLogger().LogFatal(ctx, "connection Pool Error", err)
	}

	logx.
		GetLogger().
		LogInfo(ctx, fmt.Sprintf("Created new InstanceManager Connection Pool: DB=%s, HOST=%s, PORT=%d",
			pool.Config().ConnConfig.Database,
			pool.Config().ConnConfig.Host,
			pool.Config().ConnConfig.Port))

	dbInstance = &PostgresDB{
		pool:   pool,
		dbConf: dbConf,
	}

	return dbInstance
}

func newConnectionPool(ctx context.Context, dbConf dbx.ConnConfig, preparedStatements ...dbx.PreparedStatement) (*pgxpool.Pool, error) {
	poolConfig, err := createConnectionConfiguration(dbConf)
	if err != nil {
		return nil, fmt.Errorf("error: %w", err)
	}

	// Setup prepared statements
	poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return setupPreparedStatements(ctx, conn, preparedStatements...)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, errorx.NewDatabaseErrorWrapper(err, "Error creating New Connection Pool")
	}

	return pool, nil
}

func createConnectionConfiguration(dbConf dbx.ConnConfig) (*pgxpool.Config, error) {
	poolConfig, _ := pgxpool.ParseConfig("")

	if dbConf.DBName == "" {
		return nil, errorx.NewDatabaseError("Error creating Connection Pool ConnConfig: DB_Name is EMPTY")
	}

	if dbConf.User == "" {
		return nil, errorx.NewDatabaseError("Error creating Connection Pool ConnConfig: DB_User is EMPTY")
	}

	if dbConf.Password == "" {
		return nil, errorx.NewDatabaseError("Error creating Connection Pool ConnConfig: DB_Password is EMPTY")
	}

	poolConfig.ConnConfig.Database = dbConf.DBName
	poolConfig.ConnConfig.User = dbConf.User
	poolConfig.ConnConfig.Password = dbConf.Password
	poolConfig.MaxConns = int32(runtime.NumCPU()) * dbConf.MaxConn
	poolConfig.MinConns = int32(runtime.NumCPU()) * dbConf.MaxConn
	// poolConfig.HealthCheckPeriod = config.HealthCheckPeriod
	// poolConfig.MaxConnIdleTime = config.MaxConnIdleTime
	if dbConf.IsLocalEnv || dbConf.VpcDirectConnection {
		// If local we need to specify the port, if not local
		// the port is defined in the Unix Socket configuration
		// mounted in the container at runtime (5432)
		logx.
			GetLogger().
			LogInfo(context.TODO(), fmt.Sprintf("Connecting to DB on HOST:%s and PORT:%d",
				dbConf.Host,
				uint16(dbConf.Port)))
		poolConfig.ConnConfig.Port = uint16(dbConf.Port)
		poolConfig.ConnConfig.Host = dbConf.Host
	} else {
		logx.GetLogger().LogInfo(context.TODO(), "Connecting to DB trough CLOUD SQL PROXY")
		poolConfig.ConnConfig.Host = fmt.Sprintf("/cloudsql/%s", dbConf.Host)
	}

	return poolConfig, nil
}

func setupPreparedStatements(ctx context.Context, conn *pgx.Conn, preparesStatements ...dbx.PreparedStatement) error {
	for _, stmt := range preparesStatements {
		_, err := conn.Prepare(ctx, stmt.GetName(), stmt.GetQuery())
		if err != nil {
			return errorx.NewDatabaseErrorWrapper(err, "Failed to prepare statement '%s'", stmt.GetName())
		}
	}

	return nil
}

func acquireConnectionFromPool(ctx context.Context, db *PostgresDB) (*pgxpool.Conn, error) {
	if db.pool == nil {
		logx.GetLogger().LogPanic(ctx, "error, Connection Pool To DB not initialized", nil)
	}

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		logx.GetLogger().LogError(ctx, "Error acquiring connection from pool", err)
		return nil, errors.Wrap(err, "Error acquiring connection from pool")
	}

	return conn, nil
}

// GetDbConnPool - get the connection pool.
func (dbm *PostgresDB) GetDbConnPool() (any, error) {
	if dbm.pool == nil {
		return nil, errorx.NewDatabaseError("error, Connection Pool To DB not initialized")
	}

	return dbm.pool, nil
}

// GetConnFromPool - get a connection from the pool.
func (dbm *PostgresDB) GetConnFromPool(ctx context.Context) (any, error) {
	return acquireConnectionFromPool(ctx, dbm)
}

// CloseDbConnPool - close dbx connection pool.
func (dbm *PostgresDB) CloseDbConnPool() {
	if dbm.pool != nil {
		dbm.pool.Close()
		logx.GetLogger().LogInfo(context.TODO(), "DB Connection Pool Successfully Closed!")
	}
}

// GetConnectionConfig - get Db Connection config.
func (dbm *PostgresDB) GetConnectionConfig() dbx.ConnConfig {
	return dbm.dbConf
}

// TxBegin starts a new database transaction and returns a Transaction interface that can be used to commit or roll back the transaction.
//
// This method is used to initiate a new transaction within the database. It acquires a connection from the connection pool,
// begins a transaction on that connection, and returns a `PostgresTx` struct that implements the `dbx.Transaction` interface.
// The transaction can then be used to execute multiple SQL statements as part of a single atomic operation.
//
// Arguments:
//   - ctx: The context for managing the transaction initiation, which allows for cancellation and timeouts.
//
// Returns:
//   - pgxTx: A `dbx.Transaction` interface that represents the active transaction. This can be used to execute queries within
//     the transaction and to commit or roll back the transaction.
//   - err: Any error encountered during the transaction initiation.
//
// Behavior:
//   - The method first acquires a connection from the connection pool.
//   - It then begins a transaction on the acquired connection.
//   - If the transaction is successfully started, a `PostgresTx` struct is returned, which includes the transaction object, the connection, and a randomly generated transaction ID.
//   - If there is an error during the connection acquisition or transaction initiation, the error is wrapped and returned.
//
// Example Usage:
//
//	tx, err := dbm.TxBegin(ctx)
//	if err != nil {
//	    log.Fatal("Failed to start transaction:", err)
//	}
//	defer tx.TxRollback(ctx)
//
//	// Execute queries within the transaction
//	rowsAffected, err := tx.Exec(ctx, "UPDATE users SET last_login = now() WHERE id = $1", userID)
//	if err != nil {
//	    log.Fatal("Failed to update last login time:", err)
//	}
//
//	// Commit the transaction
//	err = tx.TxCommit(ctx)
//	if err != nil {
//	    log.Fatal("Failed to commit transaction:", err)
//	}
func (dbm *PostgresDB) TxBegin(ctx context.Context) (pgxTx dbx.Transaction, err error) {
	var conn *pgxpool.Conn
	conn, err = acquireConnectionFromPool(ctx, dbm)
	if err != nil {
		return nil, err
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, errorx.NewDatabaseErrorWrapper(err, "error starting transaction")
	}

	// Generate a random Transaction ID
	txId := dbx.GenerateRandomInt64Id()

	return &PostgresTx{tx: tx, conn: conn, txId: txId}, nil
}

// Query executes a SQL query and returns both the resulting rows and the database connection.
//
// This method is designed to execute a query and return the resulting rows along with the database connection
// used to execute the query. This allows for explicit control over the connection lifecycle, ensuring that the
// connection can be properly released back to the pool after the rows have been processed.
//
// Arguments:
//   - ctx: The context for the query execution, allowing for cancellation and deadline management.
//   - distLockId: The ID used to retrieve a specific connection associated with a distributed lock. If 0,
//     it gets a connection from the general pool.
//   - query: The SQL query to be executed.
//   - args: The variadic arguments for the SQL query, if any.
//
// Returns:
//   - conn: The database connection used for the query, returned as `any`. This must be cast to the appropriate
//     connection type (e.g., `*pgxpool.Conn`) and explicitly released after use.
//   - rows: The result set from the query, returned as `any`. This must be cast to `pgx.Rows` and properly closed
//     after processing to release associated resources.
//   - err: Any error encountered during connection acquisition, query execution, or row retrieval.
//
// Usage:
//
//	After calling this method, you should ensure that both `rows` and `conn` are properly closed and released
//	to avoid connection leaks. Typically, you would use `defer` to ensure that resources are released:
//
//	conn, rows, err := dbm.Query(ctx, "SELECT * FROM my_table WHERE id = $1", 123)
//	if err != nil {
//	    // Handle error
//	}
//	defer rows.(pgx.Rows).Close()
//	defer conn.(*pgxpool.Conn).Release()
func (dbm *PostgresDB) Query(ctx context.Context, distLockId int64, query string, args ...interface{}) (conn any, rows any, err error) {
	conn, err = dbm.GetConnFromPool(ctx)
	if err != nil {
		return nil, nil, err
	}

	rows, err = conn.(*pgxpool.Conn).Query(ctx, query, args...)
	if err != nil {
		conn.(*pgxpool.Conn).Release()
		return nil, nil, err
	}

	return conn.(*pgxpool.Conn), rows.(pgx.Rows), nil
}

// Exec executes a SQL query that does not return rows, such as INSERT, UPDATE, or DELETE, and returns the number of rows affected.
//
// This method is typically used for executing SQL commands where the result is not a set of rows but a count of rows affected.
// The method acquires a database connection from the connection pool, executes the provided query, and then releases the connection back to the pool.
//
// Arguments:
//   - ctx: The context for managing the query execution, which allows for cancellation and timeouts.
//   - distLockId: An ID used to retrieve a specific connection associated with a distributed lock. This can be used for ensuring that specific operations
//     are executed on a particular connection if necessary. In this implementation, this parameter is not used, but it's included to match the signature of other methods.
//   - execQuery: The SQL query to be executed. This query should be a command that modifies data, such as an INSERT, UPDATE, DELETE, or similar operation.
//   - args: The variadic arguments that will be passed to the SQL query. These arguments will be substituted into the query at the appropriate placeholders.
//
// Returns:
//   - int64: The number of rows affected by the query.
//   - error: Any error encountered during the query execution or connection management.
//
// Behavior:
//   - The method first acquires a connection from the connection pool.
//   - It then executes the provided SQL query with the given arguments.
//   - If the execution is successful, it returns the number of rows affected by the query.
//   - If there is an error during execution, the method logs the error and returns it wrapped in a custom error type.
//   - The database connection is released back to the pool at the end of the method, regardless of success or failure.
//
// Example Usage:
//
//	rowsAffected, err := dbm.Exec(ctx, 0, "UPDATE users SET last_login = now() WHERE id = $1", userID)
//	if err != nil {
//	    log.Fatal("Failed to update last login time:", err)
//	}
//	log.Printf("%d rows were updated", rowsAffected)
func (dbm *PostgresDB) Exec(ctx context.Context, distLockId int64, execQuery string, args ...any) (int64, error) {
	var conn *pgxpool.Conn

	conn, err := acquireConnectionFromPool(ctx, dbm)
	if err != nil {
		return 0, err
	}

	defer conn.Release()

	result, err := conn.Exec(ctx, execQuery, args...)
	if err != nil {
		logx.GetLogger().LogError(ctx, fmt.Sprintf("Error executing query '%s'", execQuery), err)

		return 0, errorx.NewDatabaseErrorWrapper(err, "Error executing query '%s'", execQuery)
	}

	return result.RowsAffected(), nil
}
