package fibersrv

import (
	"context"
	"fmt"
	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2"
	"github.com/marcodd23/go-micro-core/pkg/configmgr"
	"github.com/marcodd23/go-micro-core/pkg/logmgr"
	"github.com/marcodd23/go-micro-core/pkg/servermgr"
	"sync"
)

var (
	once     sync.Once
	instance servermgr.Server[*fiber.App]
)

// FiberServer - Fiber server.
type FiberServer struct {
	Server *fiber.App
	config configmgr.Config
}

// NewFiberServer - Fiber server constructor (singleton).
func NewFiberServer(config configmgr.Config) servermgr.Server[*fiber.App] {
	once.Do(func() {
		// Setup Server
		fiberConfig := buildFiberConfig(config)
		app := fiber.New(*fiberConfig)
		instance = &FiberServer{app, config}
	})
	return instance
}

func buildFiberConfig(config configmgr.Config) *fiber.Config {
	return &fiber.Config{
		AppName:               config.GetServiceName(),
		Concurrency:           config.GetServerConfig().Concurrency,
		DisableStartupMessage: config.GetServerConfig().DisableStartupMessage,
		Prefork:               false,
		CaseSensitive:         true,
		StrictRouting:         true,
		JSONEncoder:           json.Marshal,
		JSONDecoder:           json.Unmarshal,
	}
}

// GetServer - return the fiber server.
func (srv *FiberServer) GetServer() *fiber.App {
	return srv.Server
}

// RunSync - Run the server sync.
func (srv *FiberServer) RunSync() {
	if srv.Server != nil {
		runServer(srv)
	}
}

// RunAsync - Run the server async.
func (srv *FiberServer) RunAsync() {
	if srv.Server != nil {
		go func() {
			runServer(srv)
		}()
	}
}

// Setup - Receive a callback function setupFunc that let to configure the server.
func (srv *FiberServer) Setup(ctx context.Context, setupFunc func(fiber *fiber.App)) {
	if srv.Server != nil {
		setupFunc(srv.Server)
	}
}

// Shutdown - shutdown the server.
func (srv *FiberServer) Shutdown(ctx context.Context) {
	if srv.Server != nil {
		if err := srv.Server.Server().ShutdownWithContext(ctx); err != nil {
			logmgr.GetLogger().LogError(ctx, "Error shutting down the Server", err)
		} else {
			logmgr.GetLogger().LogInfo(ctx, "Server shut down.. ")
		}
	}
}

func runServer(srv *FiberServer) {
	//appPropertyPrefix := srv.config.AppPropertyPrefix()
	serverAddr := fmt.Sprintf(":%s", srv.config.GetServerConfig().Port)
	// srv.log.LogDebugf("Serverlisten on: %s", serverAddr)
	if err := srv.Server.Listen(serverAddr); err != nil {
		logmgr.GetLogger().LogPanic(context.TODO(), "Oops... server is not running! error:", err)
	}
}
