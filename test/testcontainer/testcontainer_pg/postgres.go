package testcontainer_pg

import (
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	_ "github.com/jackc/pgx/v4/stdlib"
	//_ "github.com/golang-migrate/migrate/v4/database/pgx"
	//_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/marcodd23/go-micro-core/pkg/database"
	"github.com/marcodd23/go-micro-core/pkg/database/pgdb"
	"github.com/marcodd23/go-micro-core/pkg/logmgr"
	"github.com/marcodd23/go-micro-core/test"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"path/filepath"
	"testing"
	"time"
)

const (
	postgresContainerImage = "docker.io/postgres:16-alpine"
	postgresContainerPort  = "5432/tcp"

	MainDbName     = "main-db"
	MainDbUser     = "postgres"
	MainDbPassword = "password"
)

// PostgresContainer represents the postgres Container type used in the module.
type PostgresContainer struct {
	Container      *postgres.PostgresContainer
	MappedPort     nat.Port
	Host           string
	DbName         string
	DbUser         string
	DbPassword     string
	DbShard        database.DbShard
	PrepStatements []database.PreparedStatement
}

const TestSnapshotId = "test-snapshot"

func StartPostgresContainerWithInitScript(ctx context.Context, t *testing.T, initScriptPath string, preparesStatementsMap *database.PreparedStatementsMap) *PostgresContainer {
	test.ConfigTestRootPath()

	pg, err := postgres.Run(ctx,
		postgresContainerImage,
		postgres.WithInitScripts(filepath.Clean(initScriptPath)),
		postgres.WithDatabase(MainDbName),
		postgres.WithUsername(MainDbUser),
		postgres.WithPassword(MainDbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(10*time.Second)),
	)

	require.NoError(t, err)
	require.NotNil(t, pg)

	err = pg.Start(ctx)
	require.NoError(t, err)

	mappedPort, err := pg.MappedPort(ctx, postgresContainerPort)
	require.NoError(t, err)

	host, err := pg.Host(ctx)
	require.NoError(t, err)

	log.Printf("Postgres running at %s:%s", host, mappedPort.Port())

	// Create a snapshot of the database to restore later
	err = pg.Snapshot(ctx, postgres.WithSnapshotName(TestSnapshotId))
	require.NoError(t, err)

	return &PostgresContainer{Container: pg, MappedPort: mappedPort, Host: host, DbName: MainDbName, DbUser: MainDbUser, DbPassword: MainDbPassword, DbShard: "MAIN_DB", PrepStatements: nil}
}

// StartPostgresContainer - startContainer creates an instance of the pubsub Container type.
func StartPostgresContainer(ctx context.Context, t *testing.T, preparesStatementsMap *database.PreparedStatementsMap) *PostgresContainer {
	test.ConfigTestRootPath()

	pg, err := postgres.Run(ctx,
		postgresContainerImage,
		postgres.WithInitScripts(filepath.Join("test/testcontainer/testcontainer_pg", "init_schema.sql")),
		postgres.WithDatabase(MainDbName),
		postgres.WithUsername(MainDbUser),
		postgres.WithPassword(MainDbPassword),
		postgres.WithSQLDriver("pgx"),

		//testcontainers.WithWaitStrategy(wait.ForListeningPort(postgresContainerPort).WithStartupTimeout(5*time.Second)),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(10*time.Second)),
	)

	require.NoError(t, err)
	require.NotNil(t, pg)

	err = pg.Start(ctx)
	require.NoError(t, err)

	mappedPort, err := pg.MappedPort(ctx, postgresContainerPort)
	require.NoError(t, err)

	host, err := pg.Host(ctx)
	require.NoError(t, err)

	log.Printf("Postgres running at %s:%s", host, mappedPort.Port())

	// Create a snapshot of the database to restore later
	err = pg.Snapshot(ctx, postgres.WithSnapshotName(TestSnapshotId))
	require.NoError(t, err)

	return &PostgresContainer{Container: pg, MappedPort: mappedPort, Host: host, DbName: MainDbName, DbUser: MainDbUser, DbPassword: MainDbPassword, DbShard: "MAIN_DB", PrepStatements: nil}
}

func (c *PostgresContainer) StopContainer(ctx context.Context, t *testing.T) error {
	logmgr.GetLogger().LogInfo(ctx, "Terminating the Container ....")
	err := c.Container.Terminate(ctx)
	require.NoError(t, err, fmt.Sprintf("error terminating the Container %v", err))
	return nil
}

// SetupDatabaseConnection - Setup Database configuration for tests.
func SetupDatabaseConnection(ctx context.Context, containers ...*PostgresContainer) *database.ShardManager {
	dbShardManager := database.NewDbShardManager()

	for _, container := range containers {
		dbConf := database.ConnConfig{
			IsLocalEnv: true,
			Host:       container.Host,
			Port:       int32(container.MappedPort.Int()),
			DBName:     container.DbName,
			User:       container.DbUser,
			Password:   container.DbPassword,
			MaxConn:    10, // Increase connection pool size
		}

		db := pgdb.SetupPostgresDB(ctx, dbConf, container.PrepStatements...)

		dbInstanceManager := database.MasterReplicaManager{
			DbMaster:  db,
			DbReplica: db,
		}

		dbShardManager.AddInstanceManager(container.DbShard, dbInstanceManager)
	}

	return dbShardManager
}
