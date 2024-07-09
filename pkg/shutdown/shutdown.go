package shutdown

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/marcodd23/go-micro-core/pkg/logmgr"
)

// WaitForShutdown waits for OS signals (SIGINT, SIGTERM) to gracefully shut down the application.
// It runs the cleanup code provided by the cleanupCallback function within a context with a specified timeout.
//
// Parameters:
//   - rootCtx: The parent context.
//   - timeoutMilli: The timeout duration in milliseconds to wait for the cleanup callback to complete.
//   - cleanupCallback: A function that contains the cleanup code to execute during shutdown, and that takes a timeoutCtx.
//
// Usage:
//
//	shutdown.WaitForShutdown(context.Background(), 5000, func(timeoutCtx context.Context) {
//	    // Cleanup code here
//	    fmt.Println("Cleaning up resources...")
//	})
func WaitForShutdown(rootCtx context.Context, timeoutMilli int64, cleanupCallback func(timeoutCtx context.Context)) {
	// Handle SIGINT and SIGTERM signals
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	// capture sigterm and other system call here
	signalCaptured := <-signals
	logmgr.GetLogger().LogDebug(rootCtx, fmt.Sprintf("Interrupt signal captured: %s", signalCaptured.String()))
	// Create a context with a timeout to give time to release resource
	timeoutCtx, cancel := context.WithTimeout(rootCtx, time.Duration(timeoutMilli)*time.Millisecond)

	defer cancel()
	cleanUp(timeoutCtx, cleanupCallback)
}

// cleanUp executes the provided cleanup callback function and logs the result.
// It waits for either the cleanup to complete or the context to be cancelled.
//
// Parameters:
//   - timeoutCtx: The context to use for the cleanup callback.
//   - cleanupCallback: A function that contains the cleanup code to execute.
func cleanUp(timeoutCtx context.Context, cleanupCallback func(timeoutCtx context.Context)) {
	logmgr.GetLogger().LogInfo(timeoutCtx, "Cleaning up all resources ....")

	// Channel used to receive the result from cleanup callback function
	ch := make(chan string, 1)

	go func() {
		defer close(ch)
		if cleanupCallback != nil {
			cleanupCallback(timeoutCtx)
		}
		ch <- "All resources cleaned up"
	}()

	select {
	case <-timeoutCtx.Done():
		logmgr.GetLogger().LogError(timeoutCtx, "Deadline exceeded during context cancellation", timeoutCtx.Err())
	case result := <-ch:
		logmgr.GetLogger().LogInfo(timeoutCtx, result)
	}
}

// RunTaskWithContextCancellationCheck executes a task and provides a mechanism to notify the task of impending cancellation.
// This allows the task to perform cleanup or error handling before the context is cancelled.
// The function listens for system signals (SIGTERM, SIGINT) and sends a termination signal to the task,
// allowing it to gracefully terminate before the context is cancelled.
//
// Parameters:
//   - rootCtx: The parent context.
//   - task: A function that represents the task to be executed. It takes a cancelCtx and a termination signal channel as arguments.
//
// Usage:
//
//	shutdown.RunTaskWithContextCancellationCheck(context.Background(), func(cancelCtx context.Context, terminateSignal chan struct{}) error {
//	    for {
//	        select {
//	        case <-cancelCtx.Done():
//	            fmt.Println("Context cancelled")
//	            return cancelCtx.Err()
//	        case <-terminateSignal:
//	            fmt.Println("Received termination signal")
//	            // Perform cleanup or handle termination
//	            return nil
//	        }
//	    }
//	})
func RunTaskWithContextCancellationCheck(rootCtx context.Context, task func(cancelCtx context.Context, terminateSignal chan struct{}) error) {
	cancelCtx, cancel := context.WithCancel(rootCtx)
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigs)

	terminateSignal := make(chan struct{})
	taskCompleted := make(chan error, 1)

	go func() {
		taskCompleted <- task(cancelCtx, terminateSignal)
	}()

	select {
	case sig := <-sigs:
		fmt.Println("Received signal:", sig)
		close(terminateSignal) // Signal termination to the task
		<-taskCompleted        // Wait for the task to complete
		cancel()               // Now cancel the context
	case err := <-taskCompleted:
		// Task completed without external termination signal
		if err != nil {
			logmgr.GetLogger().LogError(cancelCtx, "Task error", err)
		}
		cancel() // Cancel the context
	}
}
