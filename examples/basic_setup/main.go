package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/marcodd23/go-micro-core/pkg/dbx"
	"github.com/marcodd23/go-micro-core/pkg/dbx/pgxdb"

	"github.com/gofiber/fiber/v2"
	"github.com/marcodd23/go-micro-core/pkg/configx"
	"github.com/marcodd23/go-micro-core/pkg/logx"
	"github.com/marcodd23/go-micro-core/pkg/serverx/fibersrv"
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

	//setupDatabase(rootCtx, config, []database.PreparedStatement{})

	serverManager := fibersrv.NewFiberServer(config)

	// Setup Routes
	serverManager.Setup(rootCtx, func(appServer *fiber.App) {
		appServer.Group("/opoa/nexus/api")
		appServer.Get("/", func(c *fiber.Ctx) error {
			return c.SendString("Hello, World!")
		})
	})

	// Start server
	serverManager.RunAsync()

	// ######### MAIN APPLICATION CODE
	wg := sync.WaitGroup{}
	appCtx, cancelAppCtx := context.WithCancel(rootCtx)
	defer cancelAppCtx()

	InfinitePolling(appCtx, &wg)
	// ###################################

	shutdown.WaitForShutdown(rootCtx, ShutdownTimeoutMilli, func(timeoutCtx context.Context) {
		serverManager.Shutdown(timeoutCtx)
		cancelAppCtx()
		wg.Wait()
	})

}

func InfinitePolling(appCtx context.Context, wg *sync.WaitGroup) {
	go func(ctx context.Context) {
		wg.Add(1)
		defer wg.Done()

		// Implement a infinite loop that just log a string, and will terminate only if the context is cancelled
		ticker := time.NewTicker(1 + time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				logx.GetLogger().LogInfo(ctx, "Context cancelled, exiting goroutine")
				return
			case <-ticker.C:
				logx.GetLogger().LogInfo(ctx, "Logging from infinite loop...")
			}
		}
	}(appCtx)
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
