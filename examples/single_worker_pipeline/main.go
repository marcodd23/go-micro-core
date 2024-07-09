package main

import (
	"context"
	"github.com/marcodd23/go-micro-core/pkg/configmgr"
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

	ExecutePipelineSimulation(appCtx, &wg)

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
