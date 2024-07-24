package main

import (
	"context"
	"fmt"
	"github.com/marcodd23/go-micro-core/pkg/logmgr"
	"github.com/marcodd23/go-micro-core/pkg/patterns/pipeline"
	"sync"
)

func SetupAndStartParallelWorkersPipeline(appCtx context.Context, wg *sync.WaitGroup) (inputChan chan pipeline.Message, outputChan chan pipeline.Message) {
	// Create input and output channels
	inputChan = make(chan pipeline.Message, 100)
	outputChan = make(chan pipeline.Message, 100)

	// Create the stages for the pipeline
	stages := []pipeline.Stage{
		pipeline.NamedStage{Name: "#FIRST#", Stage: FirstStage{Name: "#FIRST#"}},
		pipeline.NamedStage{Name: "#SECOND#", Stage: SecondStage{Name: "#SECOND#"}},
	}

	// Initialize the Orchestrator with a single pipeline configuration
	orchestrator := pipeline.NewOrchestrator(map[string]pipeline.Config{
		"pipeline1": {
			Pipeline:   pipeline.NewPipeline("pipeline1", stages),
			InputChan:  inputChan,
			OutputChan: outputChan,
			NumWorkers: 2, // Configure the number of workers as needed
		},
		"pipeline2": {
			Pipeline:   pipeline.NewPipeline("pipeline2", stages),
			InputChan:  inputChan,
			OutputChan: outputChan,
			NumWorkers: 1, // Configure the number of workers as needed
		},
	})

	// Execute the parallel pipeline
	orchestrator.Execute(appCtx, wg)

	return inputChan, outputChan
}

func StartEventsConsumerMock(appCtx context.Context, outputChan <-chan pipeline.Message) {
	// Handle the output messages if needed
	go func() {
		for msg := range outputChan {
			logmgr.GetLogger().LogDebug(appCtx, fmt.Sprintf("Output message %s received", msg.GetMsgRefId()))
		}
	}()
}

func StartEventsProducerMock(appCtx context.Context, inputChan chan<- pipeline.Message) {
	// Simulate incoming messages
	go func() {
		for i := 0; i < 10; i++ {
			msg := pipeline.NewPipelineMessage(fmt.Sprintf("messageId%d", i), []byte("message body"), map[string]string{"key": "value"}, map[string]interface{}{"attrKey": "attrValue"})
			inputChan <- msg
			logmgr.GetLogger().LogDebug(appCtx, fmt.Sprintf("Sent message %s to input channel", msg.GetMsgRefId()))
		}
		close(inputChan) // Close the input channel to signal no more messages
	}()
}

// FirstStage and Message implementations (replace with actual implementations)
type FirstStage struct {
	Name string
}

func (s FirstStage) Process(ctx context.Context, msg pipeline.Message) (pipeline.Message, error) {
	// Retrieve worker ID from the context
	//workerID, _ := ctx.Value(workerIDKey).(int)
	//logmgr.GetLogger().LogInfo(ctx, fmt.Sprintf("EXECUTING STAGE: %s", s.Name))

	// Implement the processing logic here.
	// For example, modify the message payload or attributes.

	return msg, nil
}

type SecondStage struct {
	Name string
}

func (s SecondStage) Process(ctx context.Context, msg pipeline.Message) (pipeline.Message, error) {
	// Retrieve worker ID from the context
	//workerID, _ := ctx.Value(workerIDKey).(int)
	//logmgr.GetLogger().LogInfo(ctx, fmt.Sprintf("EXECUTING STAGE: %s", s.Name))

	// Implement the processing logic here
	// For example, modify the message payload or attributes

	return msg, nil
}
