package pipeline

import (
	"context"
	"github.com/marcodd23/go-micro-core/pkg/logx"
)

// Stage describes a pipeline stage a message can be routed through.
// Depending on the implementation, it may return the same message or a modified copy.
type Stage interface {
	// Process processes an incoming message and returns a modified copy and/or error once done.
	Process(context.Context, PipeEvent) (PipeEvent, error)
}

type NamedStage struct {
	Name string
	Stage
}

func (s NamedStage) Process(ctx context.Context, msg PipeEvent) (PipeEvent, error) {
	workerID, _ := ctx.Value(workerIDKey).(string)
	pipelineName, _ := ctx.Value(pipelineNameKey).(string)

	logx.GetLogger().LogInfo(ctx, BuildPipelineLog(Processing, workerID, pipelineName, s.Name, msg.GetEventId(), ""))

	return s.Stage.Process(ctx, msg)
}
