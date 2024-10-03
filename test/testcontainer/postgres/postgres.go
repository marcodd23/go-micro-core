package postgres

import (
	"context"
	"fmt"

	"github.com/marcodd23/go-micro-core/pkg/dbx"
	"github.com/marcodd23/go-micro-core/pkg/dbx/pgxdb"

	"github.com/docker/go-connections/nat"

	"log"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/marcodd23/go-micro-core/pkg/logx"
	"github.com/marcodd23/go-micro-core/test"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
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
	DbShard        dbx.DbShard
	PrepStatements []dbx.PreparedStatement
}

const TestSnapshotId = "test-snapshot"

func StartPostgresContainerWithInitScript(ctx context.Context, t *testing.T, initScriptPath string, preparesStatements []dbx.PreparedStatement) *PostgresContainer {
	test.ConfigTestRootPath()

	pg, err := postgres.Run(ctx,
		postgresContainerImage,
		postgres.WithInitScripts(filepath.Clean(initScriptPath)),
		postgres.WithDatabase(MainDbName),
		postgres.WithUsername(MainDbUser),
		postgres.WithPassword(MainDbPassword),
		//postgres.WithSQLDriver("pgx"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(10*time.Second),
			//wait.ForListeningPort("5432/tcp"),
		),
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

	return &PostgresContainer{
		Container:      pg,
		MappedPort:     mappedPort,
		Host:           host,
		DbName:         MainDbName,
		DbUser:         MainDbUser,
		DbPassword:     MainDbPassword,
		DbShard:        "MAIN_DB",
		PrepStatements: preparesStatements,
	}
}

// StartPostgresContainer - startContainer creates an instance of the pubsub Container type.
func StartPostgresContainer(ctx context.Context, t *testing.T, preparesStatements []dbx.PreparedStatement) *PostgresContainer {
	test.ConfigTestRootPath()

	pg, err := postgres.Run(ctx,
		postgresContainerImage,
		postgres.WithInitScripts(filepath.Join("test/testcontainer/postgres", "init_schema.sql")),
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

	return &PostgresContainer{
		Container:      pg,
		MappedPort:     mappedPort,
		Host:           host,
		DbName:         MainDbName,
		DbUser:         MainDbUser,
		DbPassword:     MainDbPassword,
		DbShard:        "MAIN_DB",
		PrepStatements: preparesStatements,
	}
}

func (c *PostgresContainer) StopContainer(ctx context.Context, t *testing.T) error {
	logx.GetLogger().LogInfo(ctx, "Terminating the Container ....")

	// Define a timeout duration, for example, 10 seconds
	timeout := time.Second * 3

	// Pass the pointer to the duration (use &timeout)
	err := c.Container.Stop(ctx, &timeout)

	// Check for error when stopping the container
	if err != nil {
		require.NoError(t, err, fmt.Sprintf("error stopping the Container %v", err))
		return err
	}

	// Return nil if everything was successful
	return nil
}

// SetupDatabaseConnection - Setup Database configuration for tests.
func SetupDatabaseConnection(ctx context.Context, containers ...*PostgresContainer) *dbx.ShardManager {
	dbShardManager := dbx.NewDbShardManager()

	for _, container := range containers {
		dbConf := dbx.ConnConfig{
			IsLocalEnv: true,
			Host:       container.Host,
			Port:       int32(container.MappedPort.Int()),
			DBName:     container.DbName,
			User:       container.DbUser,
			Password:   container.DbPassword,
			MaxConn:    1, // Increase connection pool size
		}

		db := pgxdb.SetupPostgresDbManager(ctx, dbConf, container.PrepStatements...)

		dbInstanceManager := dbx.MasterReplicaManager{
			DbMaster:  db,
			DbReplica: db,
		}

		dbShardManager.AddInstanceManager(container.DbShard, dbInstanceManager)
	}

	return dbShardManager
}
