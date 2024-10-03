package main

import (
	"context"
	"log"
	"sync"

	"github.com/marcodd23/go-micro-core/pkg/dbx"
	"github.com/marcodd23/go-micro-core/pkg/dbx/pgxdb"

	"github.com/marcodd23/go-micro-core/pkg/configx"
	"github.com/marcodd23/go-micro-core/pkg/logx"
	"github.com/marcodd23/go-micro-core/pkg/shutdown"
)

// ShutdownTimeoutMilli - timeout for cleaning up resources before shutting down the server.
const ShutdownTimeoutMilli = 500

type ServiceConfig struct {
	configx.BaseConfig `mapstructure:",squash"`
	CustomProperty     string `mapstructure:"custom-property"`
}

func main() {
	rootCtx := context.Background()

	config := loadConfiguration()

	logx.SetupLogger(config)

	wg := sync.WaitGroup{}
	appCtx, cancelAppCtx := context.WithCancel(rootCtx)
	defer cancelAppCtx()

	inputChan, outputChan := SetupAndStartParallelWorkersPipeline(appCtx, &wg)

	// Run Producer
	StartEventsProducerMock(appCtx, inputChan)

	// Run Consumer
	StartEventsConsumerMock(appCtx, outputChan)

	shutdown.WaitForShutdown(rootCtx, ShutdownTimeoutMilli, func(timeoutCtx context.Context) {
		cancelAppCtx()
		wg.Wait()
	})

}

func loadConfiguration() *ServiceConfig {
	var cfg ServiceConfig

	err := configx.LoadConfigFromPathForEnv("./examples/", &cfg)
	if err != nil {
		log.Panicf("error loading property files: %+v", err)
	}

	return &cfg
}

// setupDatabase - Setup Database
func setupDatabase(
	ctx context.Context,
	appConfig *ServiceConfig,
	preparesStatements []dbx.PreparedStatement) dbx.InstanceManager {
	aclDBConf := dbx.ConnConfig{
		VpcDirectConnection: false,
		IsLocalEnv:          appConfig.IsLocalEnvironment(),
		Host:                "",
		Port:                5432,
		DBName:              "",
		User:                "",
		Password:            "",
		MaxConn:             10,
	}

	return pgxdb.SetupPostgresDbManager(ctx, aclDBConf, preparesStatements...)
}
