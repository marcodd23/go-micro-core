package pgdb

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/marcodd23/go-micro-core/pkg/database"
	"runtime"
	"sync"
	"time"

	errors "github.com/marcodd23/go-micro-core/pkg/errorx"
	"github.com/marcodd23/go-micro-core/pkg/logmgr"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	lock       sync.Mutex
	once       sync.Once
	dbInstance database.InstanceManager
)

// PostgresDB - db manager.
type PostgresDB struct {
	pool            *pgxpool.Pool
	dbConf          database.ConnConfig
	lockConnections map[int64]*pgxpool.Conn
}

// PostgresTx - Postgres Transaction manager.
type PostgresTx struct {
	tx   pgx.Tx
	conn *pgxpool.Conn
	txId int64
}

// PgxBatch - Postgres Transaction Batch manager.
type PgxBatch struct {
	batch *pgx.Batch
}

func NewEmptyBatch() database.Batch {
	return &PgxBatch{
		batch: &pgx.Batch{},
	}
}

func (ptb *PgxBatch) GetBatch() any {
	if ptb == nil {
		ptb.batch = &pgx.Batch{}
	}
	return ptb.batch
}

func (ptb *PgxBatch) Len() int {
	if ptb == nil {
		ptb.batch = &pgx.Batch{}
	}
	return ptb.batch.Len()
}

func (ptb *PgxBatch) Queue(query string, arguments ...any) {
	if ptb == nil {
		ptb.batch = &pgx.Batch{}
	}

	ptb.batch.Queue(query, arguments)
}

// PgRowScan - Wrapper around a row scanner.
type PgRowScan struct {
	row pgx.Rows
}

// Scan - fill the dest with the given row columns.
func (p *PgRowScan) Scan(dest ...any) error {
	err := p.row.Scan(dest...)
	if err != nil {
		return errors.NewDatabaseErrorWrapper(err, "error row scan")
	}

	return nil
}

// SetupPostgresDB - setupe Postgres DB connection.
func SetupPostgresDB(ctx context.Context, dbConf database.ConnConfig, preparesStatements ...database.PreparedStatement) database.InstanceManager {
	once.Do(func() {
		pool, err := newConnectionPool(ctx, dbConf, preparesStatements...)
		if err != nil {
			logmgr.GetLogger().LogFatal(ctx, "connection Pool Error", err)
		}

		logmgr.
			GetLogger().
			LogInfo(ctx, fmt.Sprintf("Created new InstanceManager Connection Pool: DB=%s, HOST=%s, PORT=%d",
				pool.Config().ConnConfig.Database,
				pool.Config().ConnConfig.Host,
				pool.Config().ConnConfig.Port))

		dbInstance = &PostgresDB{pool: pool, dbConf: dbConf}
	})

	return dbInstance
}

func newConnectionPool(ctx context.Context, dbConf database.ConnConfig, preparedStatements ...database.PreparedStatement) (*pgxpool.Pool, error) {
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
		return nil, errors.NewDatabaseErrorWrapper(err, "Error creating New Connection Pool")
	}

	return pool, nil
}

func setupPreparedStatements(ctx context.Context, conn *pgx.Conn, preparesStatements ...database.PreparedStatement) error {
	for _, stmt := range preparesStatements {
		_, err := conn.Prepare(ctx, stmt.GetName(), stmt.GetQuery())
		if err != nil {
			return errors.NewDatabaseErrorWrapper(err, "Failed to prepare statement '%s'", stmt.GetName())
		}
	}

	return nil
}

func createConnectionConfiguration(dbConf database.ConnConfig) (*pgxpool.Config, error) {
	poolConfig, _ := pgxpool.ParseConfig("")

	if dbConf.DBName == "" {
		return nil, errors.NewDatabaseError("Error creating Connection Pool ConnConfig: DB_Name is EMPTY")
	}

	if dbConf.User == "" {
		return nil, errors.NewDatabaseError("Error creating Connection Pool ConnConfig: DB_User is EMPTY")
	}

	if dbConf.Password == "" {
		return nil, errors.NewDatabaseError("Error creating Connection Pool ConnConfig: DB_Password is EMPTY")
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
		logmgr.
			GetLogger().
			LogInfo(context.TODO(), fmt.Sprintf("Connecting to DB on HOST:%s and PORT:%d",
				dbConf.Host,
				uint16(dbConf.Port)))
		poolConfig.ConnConfig.Port = uint16(dbConf.Port)
		poolConfig.ConnConfig.Host = dbConf.Host
	} else {
		logmgr.GetLogger().LogInfo(context.TODO(), "Connecting to DB trough CLOUD SQL PROXY")
		poolConfig.ConnConfig.Host = fmt.Sprintf("/cloudsql/%s", dbConf.Host)
	}

	return poolConfig, nil
}

// GetDbConnPool - get the connection pool.
func (db *PostgresDB) GetDbConnPool() (any, error) {
	if db.pool == nil {
		return nil, errors.NewDatabaseError("error, Connection Pool To DB not initialized")
	}

	return db.pool, nil
}

// GetConnFromPool - get a connection from the pool.
func (db *PostgresDB) GetConnFromPool(ctx context.Context) (any, error) {
	return acquireConnectionFromPool(ctx, db)
}

// CloseDbConnPool - close db connection pool.
func (db *PostgresDB) CloseDbConnPool() {
	if db.pool != nil {
		db.pool.Close()
		logmgr.GetLogger().LogInfo(context.TODO(), "DB Connection Pool Successfully Closed!")
	}
}

// GetConnectionConfig - get Db Connection config.
func (db *PostgresDB) GetConnectionConfig() database.ConnConfig {
	return db.dbConf
}

// Query - Execute a query and return a *ResultSet and Error, If lockId = 0 it will get a connection from pool,
// otherwise will try to retrieve the connection associated with the given lockId
// In case of we get the lockId != 0 remember that the connection won't be released, because it belongs to the of the Distributed Lock Scope
// That needs to release it
func (db *PostgresDB) Query(ctx context.Context, lockId int64, query string, args ...any) (*database.ResultSet, error) {
	var conn *pgxpool.Conn

	var err error

	if lockId == 0 {
		conn, err = acquireConnectionFromPool(ctx, db)
		if err != nil {
			return &database.ResultSet{}, err
		}

		defer conn.Release()
	} else {
		lock.Lock()
		conn = db.lockConnections[lockId]
		lock.Unlock()
		if conn == nil {
			return &database.ResultSet{}, errors.NewDatabaseError("impossible to find connection for lock advisory")
		}
	}

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		logmgr.GetLogger().LogError(ctx, fmt.Sprintf("Error executing query '%s'", query), err)

		return &database.ResultSet{}, errors.NewDatabaseErrorWrapper(err, "Error executing query '%s'", query)
	}
	defer rows.Close()

	var resultSet = database.ResultSet{}

	// Parse the rows and extract the data.
	for rows.Next() {
		rowElements, err := rows.Values()
		if err != nil {
			logmgr.GetLogger().LogError(ctx, "Error reading row values", err)

			return &database.ResultSet{}, errors.NewDatabaseErrorWrapper(err, "Error reading row values")
		}

		resultSet.RowsScan = append(resultSet.RowsScan, &PgRowScan{rows})
		resultSet.Rows = append(resultSet.Rows, rowElements)
	}

	return &resultSet, nil
}

// QueryAndProcess - Execute a query and apply the function processCallback to every row instead of returning the entire result set in memory.
// If lockId = 0 it will get a connection from pool, otherwise will try to retrieve the connection associated with the given lockId
// In case of we get the lockId != 0 remember that the connection won't be released, because it belongs to the of the Distributed Lock Scope
// That needs to release it
func (db *PostgresDB) QueryAndProcess(ctx context.Context, lockId int64, processCallback func(row database.Row) error, query string, args ...any) error {
	var conn *pgxpool.Conn

	var err error

	if lockId == 0 {
		conn, err = acquireConnectionFromPool(ctx, db)
		if err != nil {
			return err
		}

		defer conn.Release()
	} else {
		lock.Lock()
		conn = db.lockConnections[lockId]
		lock.Unlock()
		if conn == nil {
			return errors.NewDatabaseError("impossible to find connection for lock advisory")
		}
	}

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		logmgr.GetLogger().LogError(ctx, fmt.Sprintf("Error executing query '%s'", query), err)

		return errors.NewDatabaseErrorWrapper(err, "Error executing query '%s'", query)
	}
	defer rows.Close()

	// Parse the rows and extract the data.
	for rows.Next() {
		rowElements, err := rows.Values()
		if err != nil {
			logmgr.GetLogger().LogError(ctx, "Error reading row values", err)

			return errors.NewDatabaseErrorWrapper(err, "Error reading row values")
		}

		// Process the row.
		err = processCallback(rowElements)
		if err != nil {
			logmgr.GetLogger().LogError(ctx, fmt.Sprintf("Error processing row value: %v", rowElements), err)

			return errors.NewDatabaseErrorWrapper(err, fmt.Sprintf("Error processing row value: %v", rowElements))
		}
	}

	return nil
}

// Exec - execute a command query and return 1 or 0 if successful or failure. , If lockId = 0 it will get a connection from pool,
// otherwise will try to retrieve the connection associated with the given lockId
// In case of we get the lockId != 0 remember that the connection won't be released, because it belongs to the of the Distributed Lock Scope
// That needs to release it
func (db *PostgresDB) Exec(ctx context.Context, lockId int64, execQuery string, args ...any) (int64, error) {
	var conn *pgxpool.Conn

	var err error

	if lockId == 0 {
		conn, err = acquireConnectionFromPool(ctx, db)
		if err != nil {
			return 0, err
		}

		defer conn.Release()
	} else {
		lock.Lock()
		conn = db.lockConnections[lockId]
		lock.Unlock()
		if conn == nil {
			return 0, errors.NewDatabaseError("impossible to find connection for lock advisory")
		}
	}

	result, err := conn.Exec(ctx, execQuery, args...)
	if err != nil {
		logmgr.GetLogger().LogError(ctx, fmt.Sprintf("Error executing query '%s'", execQuery), err)

		return 0, errors.NewDatabaseErrorWrapper(err, "Error executing query '%s'", execQuery)
	}

	return result.RowsAffected(), nil
}

// Exec - execute a command query and return 1 or 0 if successful or failure. , If lockId = 0 it will get a connection from pool,
// otherwise will try to retrieve the connection associated with the given lockId
// In case of we get the lockId != 0 remember that the connection won't be released, because it belongs to the of the Distributed Lock Scope
// That needs to release it
func acquireConnectionFromPool(ctx context.Context, db *PostgresDB) (*pgxpool.Conn, error) {
	if db.pool == nil {
		logmgr.GetLogger().LogPanic(ctx, "error, Connection Pool To DB not initialized", nil)
	}

	conn, err := db.pool.Acquire(ctx)
	if err != nil {
		logmgr.GetLogger().LogError(ctx, "Error acquiring connection from pool", err)
		return nil, errors.NewDatabaseErrorWrapper(err, "Error acquiring connection from pool")
	}

	return conn, nil
}

// TxBegin - Begin a Transaction and return Transaction.
// If lockId = 0 it will get a connection from pool, otherwise will try to retrieve the connection associated with the given lockId
// In case of we get the lockId != 0 remember that the connection won't be released, because it belongs to the of the Distributed Lock Scope
// That needs to release it
func (db *PostgresDB) TxBegin(ctx context.Context, lockId int64) (pgxTx database.Transaction, err error) {
	var conn *pgxpool.Conn
	if lockId == 0 {
		conn, err = acquireConnectionFromPool(ctx, db)
		if err != nil {
			return nil, err
		}
	} else {
		lock.Lock()
		conn = db.lockConnections[lockId]
		lock.Unlock()
		if conn == nil {
			return nil, errors.NewDatabaseError("impossible to find connection for lock advisory")
		}
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, errors.NewDatabaseErrorWrapper(err, "error starting transaction")
	}

	// Generate a random Transaction ID using the current time as a seed
	txId := database.GenerateRandomID()

	return &PostgresTx{tx: tx, conn: conn, txId: txId}, nil
}

// ExecTransactionalTask - execute a transactional task, and optionally using the distributed lock context (if previously created). If lockId = 0 it will not use distributed lock context
func (db *PostgresDB) ExecTransactionalTask(ctx context.Context, lockId int64, task func(ctx context.Context, pgTx database.Transaction) error) error {
	pgTx, err := db.TxBegin(ctx, lockId)
	if err != nil {
		return errors.NewDatabaseErrorWrapper(err, "error starting transaction")
	}

	defer func() {
		if err != nil {
			pgTx.TxRollback(ctx, lockId)
		}
	}()

	err = task(ctx, pgTx)
	if err != nil {
		pgTx.TxRollback(ctx, 0)
		return errors.NewDatabaseErrorWrapper(err, "error executing transactional task")
	}

	err = pgTx.TxCommit(ctx, lockId)
	if err != nil {
		return errors.NewDatabaseErrorWrapper(err, "error committing transaction")
	}

	return nil
}

// ExecTaskWithDistributedLock - exec a task wrapped around a distributed lock.
// LockId is managed by the business logic and not autogenerated, so it needs to be
// passed as parameter
func (db *PostgresDB) ExecTaskWithDistributedLock(ctx context.Context, lockId int64, task func(ctx context.Context) error) (err error) {
	conn, err := acquireConnectionFromPool(ctx, db)
	if err != nil {
		return err
	}

	defer conn.Release()

	lockId, err = acquireAdvisoryLock(ctx, conn, lockId)
	if err != nil {
		return err
	}

	// Add the connection to the lock connections map
	lock.Lock()
	db.lockConnections[lockId] = conn
	lock.Unlock()

	defer func() {
		// Delete the connection from the lock connections map
		lock.Lock()
		delete(db.lockConnections, lockId)
		lock.Unlock()

		err = releaseAdvisoryLock(ctx, conn, lockId)
	}()

	err = task(ctx)
	if err != nil {
		return errors.NewDatabaseErrorWrapper(err, "error releasing advisory lock")
	}

	return nil
}

// AcquireDistributedLock acquire a distribuited lock with the given Id. It doesn't release the connection
// associated with the lockId, so it's important to release that when finishing the transaction. This is done
// by the method ReleaseDistributedLock
func (db *PostgresDB) AcquireDistributedLock(ctx context.Context, lockId int64) (int64, error) {
	if lockId == 0 {
		return 0, errors.NewDatabaseError("o is not allowed as lockId")
	}

	conn, err := acquireConnectionFromPool(ctx, db)
	if err != nil {
		return 0, err
	}

	// Add the connection to the lock connections map
	lock.Lock()
	db.lockConnections[lockId] = conn
	lock.Unlock()

	return acquireAdvisoryLock(ctx, conn, lockId)
}

func acquireAdvisoryLock(ctx context.Context, conn *pgxpool.Conn, lockId int64) (int64, error) {
	var (
		lockAcquired bool
		err          error
	)

	// Set a timeout context to prevent infinite loops
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for !lockAcquired {
		// Attempt to acquire the advisory lock
		err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lockId).Scan(&lockAcquired)
		if err != nil {
			// Log the error and wrap it with additional context
			logmgr.GetLogger().LogError(ctx, fmt.Sprintf("Error acquiring advisory lock with lockId %d", lockId), err)
			return 0, errors.NewDatabaseErrorWrapper(err, "error acquiring advisory lock with lockId %d", lockId)
		}

		// Check if the context is done to handle timeouts
		if ctx.Err() != nil {
			// Log the timeout error
			logmgr.GetLogger().LogError(ctx, fmt.Sprintf("Timeout while attempting to acquire advisory lock with lockId %d", lockId), ctx.Err())
			return 0, errors.NewDatabaseErrorWrapper(ctx.Err(), "timeout while attempting to acquire advisory lock with lockId %d", lockId)
		}
	}

	// Successfully acquired the lock
	return lockId, nil
}

func (db *PostgresDB) ReleaseDistributedLock(ctx context.Context, lockId int64) error {
	if conn, exist := db.lockConnections[lockId]; exist {
		defer conn.Release()
		return releaseAdvisoryLock(ctx, conn, lockId)
	} else {
		logmgr.GetLogger().LogError(ctx, fmt.Sprintf("no connection found for the lockId: %d", lockId))
		return errors.NewDatabaseError("no connection found for the lockId: %d", lockId)
	}
}

// ReleaseDistributedLock - Release a distribute lock.
func releaseAdvisoryLock(ctx context.Context, conn *pgxpool.Conn, lockId int64) error {
	_, err := conn.Query(ctx, "SELECT pg_advisory_unlock($1)", lockId)
	if err != nil {
		logmgr.GetLogger().LogError(ctx, fmt.Sprintf("error releasing Advisory lock: %d", lockId), err)
		return errors.NewDatabaseErrorWrapper(err, "error releasing Advisory lock: %d", lockId)
	}

	return nil
}

// TxCommit - Commit a transaction and release the connection to the pool.
func (t *PostgresTx) TxCommit(ctx context.Context, lockId int64) error {
	if lockId == 0 {
		defer t.conn.Release()
	}

	err := t.tx.Commit(ctx)
	if err != nil {
		logmgr.GetLogger().LogError(ctx, "error during transaction commit", err)
		return errors.NewDatabaseErrorWrapper(err, "error during transaction commit")
	}

	return nil
}

// TxRollback - Rollback a transaction and release the connection to the pool.
func (t *PostgresTx) TxRollback(ctx context.Context, lockId int64) {
	if lockId == 0 {
		defer t.conn.Release()
	}

	err := t.tx.Rollback(ctx)
	if err != nil {
		logmgr.GetLogger().LogError(ctx, fmt.Sprintf("error Rolling Back transaction: %d", t.txId), err)
	} else {
		logmgr.GetLogger().LogDebug(ctx, fmt.Sprintf("Rollack transaction: %d", t.txId))
	}
}

// TxQuery - Execute a query under a Transaction and return a *ResultSet and Error.
func (t *PostgresTx) TxQuery(ctx context.Context, query string, args ...any) (*database.ResultSet, error) {
	rows, err := t.tx.Query(ctx, query, args...)
	if err != nil {
		return &database.ResultSet{}, errors.NewDatabaseErrorWrapper(err, "Error executing query '%s'", query)
	}
	defer rows.Close()

	var resultSet = database.ResultSet{}

	// Parse the rows and extract the data.
	for rows.Next() {
		rowElements, err := rows.Values()
		if err != nil {
			logmgr.GetLogger().LogError(ctx, "Error reading row value", err)
			return &database.ResultSet{}, errors.NewDatabaseErrorWrapper(err, "Error reading row value")
		}

		resultSet.RowsScan = append(resultSet.RowsScan, &PgRowScan{rows})
		resultSet.Rows = append(resultSet.Rows, rowElements)
	}

	return &resultSet, nil
}

// TxExec - Execute a command query under a Transaction and return 1 or 0 if successful or failure.
func (t *PostgresTx) TxExec(ctx context.Context, execQuery string, args ...any) (int64, error) {
	result, err := t.tx.Exec(ctx, execQuery, args...)
	if err != nil {
		return 0, errors.NewDatabaseErrorWrapper(err, "Error executing query '%s'", execQuery)
	}

	return result.RowsAffected(), nil
}

// TxExecBatch - Execute batch query in single transaction.
func (t *PostgresTx) TxExecBatch(ctx context.Context, batch database.Batch) (int64, error) {
	pgxBatch := batch.GetBatch().(*pgx.Batch)
	batchResult := t.tx.SendBatch(ctx, pgxBatch)

	var err error

	var ct pgconn.CommandTag

	var rowUpdated int64

	defer func() {
		err = batchResult.Close()
	}()

	for i := 0; i < pgxBatch.Len() && err == nil; i++ {
		ct, err = batchResult.Exec()
		rowUpdated += ct.RowsAffected()
	}

	if err != nil {
		return 0, errors.NewGeneralErrorWrapper(err, "transaction failed")
	}

	return rowUpdated, nil
}
