package pipeline

import (
	"context"
	"fmt"
	"github.com/marcodd23/go-micro-core/pkg/logmgr"
)

// A Pipeline helps orchestrate multiple pipeline steps.
type Pipeline struct {
	Name   string
	stages []Stage
}

// NewPipeline creates a new pipeline including the stages as configured.
func NewPipeline(name string, stages []Stage) *Pipeline {
	return &Pipeline{name, stages}
}

// Process pipes an incoming message through the pipeline.
// Parameters:
//   - ctx (context.Context): Processing context. Used for tracing.
//   - msg (messaging.Message): Message to process.
//
// Returns:
//   - messaging.Message: Processed message. Since incoming messages are immutable, this is an updated copy.
//   - error: If any error occurs during processing, this will not be nil.
func (p Pipeline) Process(ctx context.Context, msg Message) (Message, error) {
	workerID, _ := ctx.Value(workerIDKey).(string)
	pipelineName, _ := ctx.Value(pipelineNameKey).(string)
	logmgr.GetLogger().LogInfo(ctx, buildPipelineStartAndStageLog(true, workerID, pipelineName, "", msg.GetMsgRefId()))

	var message = msg
	var err error

	// route message through all pipeline stages, break on error
	for _, stage := range p.stages {
		message, err = stage.Process(ctx, message)
		if err != nil {
			break
		}
	}

	return message, err
}

func buildPipelineStartAndStageLog(isStarting bool, workerID string, pipelineName string, stageName string, msgId string) string {

	logMessage := ""
	if isStarting {
		logMessage += fmt.Sprintf("Starting ")
	}
	if pipelineName != "" {
		logMessage += fmt.Sprintf("Pipeline: %s, ", pipelineName)
	}
	if workerID != "" {
		logMessage += fmt.Sprintf("Worker: %s, ", workerID)
	}
	if stageName != "" {
		logMessage += fmt.Sprintf("Stage: %s, ", stageName)
	}
	if msgId != "" {
		logMessage += fmt.Sprintf("Processing message: %s", msgId)
	}

	return logMessage
}
