package pipeline

import (
	"context"
	"github.com/marcodd23/go-micro-core/pkg/logmgr"
)

// Stage describes a pipeline stage a message can be routed through.
// Depending on the implementation, it may return the same message or a modified copy.
type Stage interface {
	// Process processes an incoming message and returns a modified copy and/or error once done.
	Process(context.Context, Message) (Message, error)
}

type NamedStage struct {
	Name string
	Stage
}

func (s NamedStage) Process(ctx context.Context, msg Message) (Message, error) {
	workerID, _ := ctx.Value(workerIDKey).(string)
	pipelineName, _ := ctx.Value(pipelineNameKey).(string)

	logmgr.GetLogger().LogInfo(ctx, buildPipelineStartAndStageLog(false, workerID, pipelineName, s.Name, msg.GetMsgRefId()))

	return s.Stage.Process(ctx, msg)
}
