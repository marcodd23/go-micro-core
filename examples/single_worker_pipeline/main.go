package main

import (
	"context"
	"log"
	"sync"

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

	// Setup the simple single worker pipeline
	pipelineInputCh := SetupAndStartPipeline(appCtx, &wg)

	SimulateEventsProducer(appCtx, pipelineInputCh)

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
