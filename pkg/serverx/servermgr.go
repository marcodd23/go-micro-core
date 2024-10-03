package serverx

import (
	"context"
)

// Server - server interface.
type Server[T any] interface {
	RunSync()
	RunAsync()
	GetServer() T
	Setup(ctx context.Context, setupFunc func(server T))
	Shutdown(ctx context.Context)
}
