package pipeline

import (
	"context"
	"fmt"

	"github.com/marcodd23/go-micro-core/pkg/logmgr"
)

// Status Define the Pipeline Status enum
type Status int

const (
	Starting Status = iota
	Processing
	Completed
	Error
	Stopped
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
//   - msg (Message): ImmutablePipeMessage to process.
//
// Returns:
//   - Message: Processed message. Since incoming messages are immutable, this is an updated copy.
//   - error: If any error occurs during processing, this will not be nil.
func (p Pipeline) Process(ctx context.Context, msg Message) (Message, error) {
	workerID, _ := ctx.Value(workerIDKey).(string)
	pipelineName, _ := ctx.Value(pipelineNameKey).(string)
	logmgr.GetLogger().LogInfo(ctx, BuildPipelineLog(
		Starting,
		workerID,
		pipelineName,
		"",
		msg.GetMsgRefId(),
		""))

	var message = msg
	var err error

	// route message through all pipeline stages, break on error
	for _, stage := range p.stages {
		message, err = stage.Process(ctx, message)
		if err != nil {
			break
		}
	}

	if err == nil {
		logmgr.GetLogger().LogInfo(ctx, BuildPipelineLog(
			Completed,
			workerID,
			pipelineName,
			"",
			msg.GetMsgRefId(),
			"COMPLETED"))
	}

	return message, err
}

func BuildPipelineLog(status Status, workerID string, pipelineName string, stageName string, msgId string, customMsg string) string {

	logMessage := ""

	switch status {
	case Starting:
		logMessage += fmt.Sprintf("Starting ")
	case Processing:
		logMessage += fmt.Sprintf("Processing ")
	case Completed:
		logMessage += fmt.Sprintf("Completed ")
	case Error:
		logMessage += fmt.Sprintf("Error ")
	case Stopped:
		logMessage += fmt.Sprintf("Stopped ")
	}

	if pipelineName != "" {
		logMessage += fmt.Sprintf("#%s Pipeline -- ", pipelineName)
	} else {
		logMessage += fmt.Sprintf("Pipeline -- ")
	}

	if workerID != "" {
		logMessage += fmt.Sprintf("#Worker: %s, ", workerID)
	}
	if stageName != "" && status != Completed && status != Starting {
		logMessage += fmt.Sprintf("#Stage: %s, ", stageName)
	}
	if msgId != "" {
		logMessage += fmt.Sprintf("#MessageKey: %s", msgId)
	}
	if customMsg != "" {
		logMessage += fmt.Sprintf(" -- %s", customMsg)
	}

	return logMessage
}
