package main

import (
	"context"
	"github.com/marcodd23/go-micro-core/pkg/configmgr"
	"github.com/marcodd23/go-micro-core/pkg/database"
	"github.com/marcodd23/go-micro-core/pkg/database/pgdb"
	"github.com/marcodd23/go-micro-core/pkg/logmgr"
	"github.com/marcodd23/go-micro-core/pkg/shutdown"
	"log"
	"sync"
)

// ShutdownTimeoutMilli - timeout for cleaning up resources before shutting down the server.
const ShutdownTimeoutMilli = 500

type ServiceConfig struct {
	configmgr.BaseConfig `mapstructure:",squash"`
	CustomProperty       string `mapstructure:"custom-property"`
}

func main() {
	rootCtx := context.Background()

	config := loadConfiguration()

	logmgr.SetupLogger(config)

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

	err := configmgr.LoadConfigFromPathForEnv("./examples/", &cfg)
	if err != nil {
		log.Panicf("error loading property files: %+v", err)
	}

	return &cfg
}

// setupDatabase - Setup Database
func setupDatabase(
	ctx context.Context,
	appConfig *ServiceConfig,
	preparesStatements []database.PreparedStatement) database.InstanceManager {
	aclDBConf := database.ConnConfig{
		VpcDirectConnection: false,
		IsLocalEnv:          appConfig.IsLocalEnvironment(),
		Host:                "",
		Port:                5432,
		DBName:              "",
		User:                "",
		Password:            "",
		MaxConn:             10,
	}

	return pgdb.SetupPostgresDB(ctx, aclDBConf, preparesStatements...)
}
