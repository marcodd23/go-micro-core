package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/marcodd23/go-micro-core/pkg/logx"
	"github.com/marcodd23/go-micro-core/pkg/patterns/pipeline"
)

func SetupAndStartParallelWorkersPipeline(appCtx context.Context, wg *sync.WaitGroup) (inputChan chan pipeline.PipeEvent, outputChan chan pipeline.PipeEvent) {
	// Create input and output channels
	inputChan = make(chan pipeline.PipeEvent, 100)
	outputChan = make(chan pipeline.PipeEvent, 100)

	// Create the stages for the pipeline
	stages := []pipeline.Stage{
		pipeline.NamedStage{Name: "FIRST", Stage: FirstStage{}},
		pipeline.NamedStage{Name: "SECOND", Stage: SecondStage{}},
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

func StartEventsConsumerMock(appCtx context.Context, outputChan <-chan pipeline.PipeEvent) {
	// Handle the output messages if needed
	go func() {
		for msg := range outputChan {
			logx.GetLogger().LogDebug(appCtx, fmt.Sprintf("Output message %s received", msg.GetEventId()))
		}
	}()
}

func StartEventsProducerMock(appCtx context.Context, inputChan chan<- pipeline.PipeEvent) {
	// Simulate incoming messages
	go func() {
		for i := 0; i < 10; i++ {
			msg := pipeline.NewImmutablePipeMessage(fmt.Sprintf("messageId%d", i), []byte("message body"), map[string]string{"attrKey": "attrValue"})
			inputChan <- msg
			logx.GetLogger().LogDebug(appCtx, fmt.Sprintf("Sent message %s to input channel", msg.GetEventId()))
		}
		close(inputChan) // Close the input channel to signal no more messages
	}()
}

// FirstStage Stage and ImmutablePipeEvent implementations (replace with actual implementations)
type FirstStage struct {
}

func (s FirstStage) Process(ctx context.Context, msg pipeline.PipeEvent) (pipeline.PipeEvent, error) {
	// Retrieve worker ID from the context
	//workerID, _ := ctx.Value(workerIDKey).(int)
	//logx.GetLogger().LogInfo(ctx, fmt.Sprintf("EXECUTING STAGE: %s", s.Name))

	// Implement the processing logic here
	// For example, modify the message payload or attributes

	return msg, nil
}

// SecondStage Stage and ImmutablePipeEvent implementations (replace with actual implementations)
type SecondStage struct {
}

func (s SecondStage) Process(ctx context.Context, msg pipeline.PipeEvent) (pipeline.PipeEvent, error) {
	// Retrieve worker ID from the context
	//workerID, _ := ctx.Value(workerIDKey).(int)
	//logx.GetLogger().LogInfo(ctx, fmt.Sprintf("EXECUTING STAGE: %s", s.Name))

	// Implement the processing logic here
	// For example, modify the message payload or attributes

	return msg, nil
}
