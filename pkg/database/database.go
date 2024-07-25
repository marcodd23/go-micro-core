//nolint:gochecknoglobals
package database

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"github.com/marcodd23/go-micro-core/pkg/errorx"
	"github.com/marcodd23/go-micro-core/pkg/logmgr"
	"math"
)

// =====================================
// InstanceManager Interface
// =====================================

// InstanceManager - InstanceManager interface.
type InstanceManager interface {
	GetDbConnPool() (any, error)
	GetConnFromPool(ctx context.Context) (any, error)
	CloseDbConnPool()
	GetConnectionConfig() ConnConfig
	Query(ctx context.Context, lockId int64, query string, args ...any) (ResultSet, error)
	QueryAndClose(ctx context.Context, lockId int64, query string, args ...any) (ResultSet, error)
	QueryAndProcess(ctx context.Context, lockId int64, processCallback func(row Row, rowScan RowScan) error, query string, args ...any) error
	Exec(ctx context.Context, lockId int64, execQuery string, args ...any) (int64, error)
	TxBegin(ctx context.Context, lockId int64) (Transaction, error)
	ExecTransactionalTask(ctx context.Context, lockId int64, task func(ctx context.Context, tx Transaction) error) error
	AcquireDistributedLock(ctx context.Context, lockId int64) (int64, error)
	ReleaseDistributedLock(ctx context.Context, lockId int64) error
	ExecTaskWithDistributedLock(ctx context.Context, lockId int64, task func(ctx context.Context) error) error
}

// =====================================
// Transaction Interface
// =====================================

// Transaction - transaction interface.
type Transaction interface {
	TxCommit(ctx context.Context, lockId int64) error
	TxRollback(ctx context.Context, lockId int64)
	TxQuery(ctx context.Context, query string, args ...any) (ResultSet, error)
	TxQueryAndClose(ctx context.Context, query string, args ...any) (ResultSet, error)
	TxExec(ctx context.Context, query string, args ...any) (int64, error)
	TxExecBatch(ctx context.Context, batch Batch) (int64, error)
}

// =====================================
// ConnConfig Definition
// =====================================

// ConnConfig - InstanceManager configuration.
type ConnConfig struct {
	VpcDirectConnection bool
	Host                string
	Port                int32
	DBName              string
	User                string
	Password            string
	MaxConn             int32
	IsLocalEnv          bool
}

// =====================================
// Batch Interface
// =====================================

// Batch - transaction interface.
type Batch interface {
	GetBatch() any
	Len() int
	Queue(query string, arguments ...any)
}

// ============================================
// Sharding and Master Replica manager structs
// ============================================

// DbShard - represent the shard instance key.
type DbShard string

// ShardManager - it manage a pool of db shards, keeping them in a map. Each map value is a MasterReplicaManager.
type ShardManager struct {
	DbShardMap map[DbShard]MasterReplicaManager
}

// MasterReplicaManager - it manage master and read replica instance.
type MasterReplicaManager struct {
	DbMaster  InstanceManager
	DbReplica InstanceManager
}

// NewDbShardManager is a constructor that ensures the inner map is always initialized.
func NewDbShardManager() *ShardManager {
	return &ShardManager{
		DbShardMap: make(map[DbShard]MasterReplicaManager),
	}
}

// AddInstanceManager adds a new MasterReplicaManager to the given DbShard.
func (dsm *ShardManager) AddInstanceManager(dbShard DbShard, manager MasterReplicaManager) {
	if dsm.DbShardMap == nil {
		dsm.DbShardMap = make(map[DbShard]MasterReplicaManager)
	}

	dsm.DbShardMap[dbShard] = manager
}

// ============================================
// Prepared Statements structs
// ============================================

// PreparedStatement - Prepared statement query.
type PreparedStatement struct {
	Name  string
	Query string
}

// PreparedStatementsMap - map of DbShard and relative PreparedStatements.
type PreparedStatementsMap struct {
	DbPrepStmMap map[DbShard][]PreparedStatement
}

// NewPreparedStatement - Create new Prepared Statement.
func NewPreparedStatement(name, query string) PreparedStatement {
	return PreparedStatement{Name: name, Query: query}
}

// GetName - name of the prepared statement.
func (p PreparedStatement) GetName() string {
	return p.Name
}

// GetQuery - query of the prepared statement.
func (p PreparedStatement) GetQuery() string {
	return p.Query
}

// ============================================
// DefaultResultSet, Row and RowScan structs
// ============================================

// Row represents a testcontainer_pg row returned as a result.
type Row []any

// RowScan represents a row that can be mapped to dest fields trough Scan function.
type RowScan interface {
	Scan(dest ...any) error
}

type ResultSet interface {
	GetRow(rowIdx int) (Row, error)
	GetRows() []Row
	GetRowScan(rowIdx int) (RowScan, error)
	GetRowsScan() []RowScan
	Close()
}

// DefaultResultSet represents the query result set.
type DefaultResultSet struct {
	Rows     []Row
	RowsScan []RowScan
}

// GetRow - get row by index.
func (r *DefaultResultSet) GetRow(rowIdx int) (Row, error) {
	if rowIdx < 0 || rowIdx >= len(r.Rows) {
		return Row{}, errorx.NewDatabaseError("Error retrieving DefaultResultSet row, index out of range: %d", rowIdx)
	}

	return r.Rows[rowIdx], nil
}

// GetRows - return all the Row of this resultset.
func (r *DefaultResultSet) GetRows() []Row {
	return r.Rows
}

// GetRowScan - Get row scan.
func (r *DefaultResultSet) GetRowScan(rowIdx int) (RowScan, error) {
	if rowIdx < 0 || rowIdx >= len(r.RowsScan) {
		return nil, errorx.NewDatabaseError("Error retrieving DefaultResultSet RowsScan, index out of range: %d", rowIdx)
	}

	return r.RowsScan[rowIdx], nil
}

// GetRowsScan - Return all RowScan of this resultset
func (r *DefaultResultSet) GetRowsScan() []RowScan {
	return r.RowsScan
}

// Close - It supposed to be implemented by the derived struct
// to close the resultset eventually (Rows, RowsScan)
func (r *DefaultResultSet) Close() {}

// ============================================
// Utility Functions
// ============================================

func GenerateRandomID() int64 {
	var idNum uint64

	for idNum == 0 {
		err := binary.Read(rand.Reader, binary.BigEndian, &idNum)
		if err != nil {
			logmgr.GetLogger().LogError(context.TODO(), "error generating 64-bit random ID", err)
			continue
		}

		idNum %= uint64(math.MaxInt64)
	}

	return int64(idNum)
}
