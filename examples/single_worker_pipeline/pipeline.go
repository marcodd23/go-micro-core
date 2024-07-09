package main

import (
	"context"
	"fmt"
	"github.com/marcodd23/go-micro-core/pkg/logmgr"
	"github.com/marcodd23/go-micro-core/pkg/patterns/pipeline"
	"sync"
)

func StartProducer(appCtx context.Context, inputChan chan<- pipeline.Message) {
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

func ExecutePipelineSimulation(appCtx context.Context, wg *sync.WaitGroup) {
	// Create pipeline stages and pipeline
	stages := []pipeline.Stage{
		&pipeline.NamedStage{Name: "# FIRST #", Stage: &FirstStage{Name: "# FIRST #"}},
		&pipeline.NamedStage{Name: "# SECOND #", Stage: &SecondStage{Name: "# FIRST #"}},
	}

	pipe := pipeline.NewPipeline("Pipeline-1", stages)

	// Create a channel for incoming messages
	inputChannel := make(chan pipeline.Message, 100) // Buffer size of 100

	// Start one goroutine to process messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		processMessages(appCtx, pipe, inputChannel)
	}()

	// Simulate incoming messages
	// Simulate incoming messages
	StartProducer(appCtx, inputChannel)
}

func processMessages(appCtx context.Context, pipe *pipeline.Pipeline, msgs <-chan pipeline.Message) {
	for msg := range msgs {
		logmgr.GetLogger().LogDebug(appCtx, fmt.Sprintf("Starting processing message %s", msg.GetMsgRefId()))
		processedMsg, err := pipe.Process(appCtx, msg)
		if err != nil {
			logmgr.GetLogger().LogError(appCtx, fmt.Sprintf("Processing message %s: %v", msg.GetMsgRefId(), err))
			continue
		}
		logmgr.GetLogger().LogInfo(appCtx, fmt.Sprintf("Finished processing message %s successfully", processedMsg.GetMsgRefId()))
	}
}

// Stage and Message implementations (replace with actual implementations)
type FirstStage struct {
	Name string
}

func (s *FirstStage) Process(ctx context.Context, msg pipeline.Message) (pipeline.Message, error) {
	// Retrieve worker ID from the context
	//workerID, _ := ctx.Value(workerIDKey).(int)
	//logmgr.GetLogger().LogInfo(ctx, fmt.Sprintf("Worker %d processing message %s at stage %s", workerID, msg.GetMsgRefId(), s.Name))
	logmgr.GetLogger().LogInfo(ctx, fmt.Sprintf("EXECUTING STAGE: %s", s.Name))

	// Implement your processing logic here
	// For example, modify the message payload or attributes
	return msg, nil
}

type SecondStage struct {
	Name string
}

func (s *SecondStage) Process(ctx context.Context, msg pipeline.Message) (pipeline.Message, error) {
	// Retrieve worker ID from the context
	//workerID, _ := ctx.Value(workerIDKey).(int)
	//logmgr.GetLogger().LogInfo(ctx, fmt.Sprintf("Worker %d processing message %s at stage %s", workerID, msg.GetMsgRefId(), s.Name))

	logmgr.GetLogger().LogInfo(ctx, fmt.Sprintf("EXECUTING STAGE: %s", s.Name))

	// Implement your processing logic here
	// For example, modify the message payload or attributes
	return msg, nil
}
