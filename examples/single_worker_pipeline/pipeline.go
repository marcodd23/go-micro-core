package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/marcodd23/go-micro-core/pkg/logx"
	"github.com/marcodd23/go-micro-core/pkg/patterns/pipeline"
)

func SetupAndStartPipeline(appCtx context.Context, wg *sync.WaitGroup) chan<- pipeline.PipeEvent {
	// Create pipeline stages and pipeline
	stages := []pipeline.Stage{
		&pipeline.NamedStage{Name: "FIRST", Stage: &FirstStage{name: "FIRST"}},
		&pipeline.NamedStage{Name: "SECOND", Stage: &SecondStage{name: "SECOND"}},
	}

	pipe := pipeline.NewPipeline("Pipeline-1", stages)

	// Create a channel for incoming messages
	inputChannel := make(chan pipeline.PipeEvent, 100) // Buffer size of 100

	// Start one goroutine to process messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		processMessages(appCtx, pipe, inputChannel)
	}()

	// Return the pipeline inputChannel
	return inputChannel
}

func processMessages(appCtx context.Context, pipe *pipeline.Pipeline, msgs <-chan pipeline.PipeEvent) {
	for msg := range msgs {
		logx.GetLogger().LogDebug(appCtx, fmt.Sprintf("Start PIPELINE for message: %s", msg.GetEventId()))
		processedMsg, err := pipe.Process(appCtx, msg)
		if err != nil {
			logx.GetLogger().LogError(appCtx, fmt.Sprintf("error pipeline for message: %s: %v", msg.GetEventId(), err))
			continue
		}
		logx.GetLogger().LogInfo(appCtx, pipeline.BuildPipelineLog(
			pipeline.Completed,
			"",
			"",
			"",
			processedMsg.GetEventId(),
			""))
	}
}

func SimulateEventsProducer(appCtx context.Context, inputChan chan<- pipeline.PipeEvent) {
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

// Stage and ImmutablePipeEvent implementations (replace with actual implementations)
type FirstStage struct {
	name string
}

func (s *FirstStage) Process(ctx context.Context, msg pipeline.PipeEvent) (pipeline.PipeEvent, error) {
	// Retrieve worker ID from the context
	//workerID, _ := ctx.Value(workerIDKey).(int)
	//logx.GetLogger().LogInfo(ctx, fmt.Sprintf("EXECUTING STAGE: %s", s.Name))
	logx.GetLogger().LogInfo(ctx,
		pipeline.BuildPipelineLog(
			pipeline.Processing,
			"",
			"",
			s.name,
			msg.GetEventId(),
			"Processing First Stage"))

	// Implement your processing logic here
	// For example, modify the message payload or attributes
	return msg, nil
}

type SecondStage struct {
	name string
}

func (s *SecondStage) Process(ctx context.Context, msg pipeline.PipeEvent) (pipeline.PipeEvent, error) {
	// Retrieve worker ID from the context
	//workerID, _ := ctx.Value(workerIDKey).(int)
	//logx.GetLogger().LogInfo(ctx, fmt.Sprintf("EXECUTING STAGE: %s", s.Name))
	logx.GetLogger().LogInfo(ctx,
		pipeline.BuildPipelineLog(
			pipeline.Processing,
			"",
			"",
			s.name,
			msg.GetEventId(),
			"Processing Second Stage"))

	// Implement your processing logic here
	// For example, modify the message payload or attributes
	return msg, nil
}
