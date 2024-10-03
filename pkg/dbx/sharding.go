package dbx

// DbShard represents the shard instance key.
//
// It represents a specific shard of the database and the DbShard type is used as a key to uniquely
// identify and manage these shards.
type DbShard string

// ShardManager manages a pool of database shards, keeping them in a map.
//
// The ShardManager is responsible for organizing and managing multiple database shards. Each shard is associated
// with a MasterReplicaManager, which manages the master and replica instances for that shard. This allows the
// ShardManager to efficiently route database operations to the appropriate shard and instance.
//
// Fields:
//   - DbShardMap: A map where the key is a DbShard, and the value is a MasterReplicaManager responsible for managing
//     the master and replica instances of that shard.
type ShardManager struct {
	DbShardMap map[DbShard]MasterReplicaManager
}

// NewDbShardManager is a constructor that ensures the inner map is always initialized.
//
// This function creates a new ShardManager instance with an initialized map. This ensures that the ShardManager
// is ready to manage shards immediately upon creation, avoiding potential nil pointer errors when adding shards.
//
// Returns:
//   - *ShardManager: A new instance of the ShardManager struct.
func NewDbShardManager() *ShardManager {
	return &ShardManager{
		DbShardMap: make(map[DbShard]MasterReplicaManager),
	}
}

// MasterReplicaManager manages master and read replica instances.
//
// The MasterReplicaManager is responsible for managing the master database instance and its read replicas
// for a particular shard. It provides a structure for routing read and write operations to the appropriate
// database instances based on the operation type.
//
// Fields:
//   - DbMaster: An InstanceManager responsible for managing the master database instance, which handles all write operations.
//   - DbReplica: An InstanceManager responsible for managing the read replica database instance, which handles read operations.
type MasterReplicaManager struct {
	DbMaster  InstanceManager
	DbReplica InstanceManager
}

// AddInstanceManager adds a new MasterReplicaManager to the given DbShard.
//
// This method adds a MasterReplicaManager to the ShardManager's internal map, associating it with a specific DbShard.
// If the map is uninitialized (which should not happen if the ShardManager is created using NewDbShardManager), it initializes
// the map before adding the entry.
//
// Arguments:
//   - dbShard: The DbShard key to which the MasterReplicaManager will be associated.
//   - manager: The MasterReplicaManager that manages the master and replica instances for the shard.
//
// Example Usage:
//
//	shardManager := NewDbShardManager()
//	shardManager.AddInstanceManager("shard1", masterReplicaManager)
func (dsm *ShardManager) AddInstanceManager(dbShard DbShard, manager MasterReplicaManager) {
	if dsm.DbShardMap == nil {
		dsm.DbShardMap = make(map[DbShard]MasterReplicaManager)
	}

	dsm.DbShardMap[dbShard] = manager
}
