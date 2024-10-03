package profiling

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

// SetupProfileFile checks if the profile file exists and creates a new file if necessary.
// If the file already exists, it appends a timestamp to avoid overwriting.
func SetupProfileFile(filename string) (*os.File, error) {
	// Check if the file already exists
	if _, err := os.Stat(filename); err == nil {
		// If it exists, append a timestamp to create a unique file
		timestamp := time.Now().Format("20060102_150405")
		filename = fmt.Sprintf("%s_%s", filename, timestamp)
	}

	// Create or open the file
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("could not create profile file: %w", err)
	}

	return file, nil
}

// StartCPUProfile starts CPU profiling and writes the profiling data to the specified file.
// It returns a function to stop the profiling. If profiling is disabled, it returns a no-op function.
//
// Parameters:
// - filename: The name of the file to write the CPU profile data.
// - enabled: A boolean indicating whether profiling is enabled.
//
// Returns:
// - stopCPUProfile: A function that stops the CPU profiling when called.
func StartCPUProfile(filename string, enabled bool) (stopCPUProfile func()) {
	if enabled {
		f, err := SetupProfileFile(filename)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}

		if err := pprof.StartCPUProfile(f); err != nil {
			f.Close()
			log.Fatal("could not start CPU profile: ", err)
		}

		return func() {
			pprof.StopCPUProfile()
			f.Close()
		}
	}

	return func() {}
}

// CPUProfile runs a CPU profile while executing the provided function. It writes profiling data
// to the specified file if profiling is enabled.
//
// Parameters:
// - filename: The name of the file to write the CPU profile data.
// - enabled: A boolean indicating whether profiling is enabled.
// - targetFunc: A function to execute while profiling.
//
// Returns:
// - error: Returns any error encountered during the execution of the target function or profiling.
func CPUProfile(filename string, enabled bool, targetFunc func() error) error {
	if enabled {
		f, err := SetupProfileFile(filename)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}

		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			f.Close()
			log.Fatal("could not start CPU profile: ", err)
		}

		err = targetFunc()

		pprof.StopCPUProfile()

		if err != nil {
			log.Printf("error executing CPUProfile target function: %s", err.Error())
			return fmt.Errorf("error executing CPUProfile target function: %w", err)
		}

		return nil
	}

	return targetFunc()
}

// CaptureMEMProfile captures a memory profile and writes it to the specified file if profiling is enabled.
// This triggers garbage collection to ensure the memory profile is accurate.
//
// Parameters:
// - filename: The name of the file to write the memory profile data.
// - enabled: A boolean indicating whether profiling is enabled.
func CaptureMEMProfile(filename string, enabled bool) {
	if enabled {
		f, err := SetupProfileFile(filename)
		if err != nil {
			log.Fatal("could not create MEM profile: ", err)
		}

		// Trigger garbage collection to ensure memory profiling is accurate
		runtime.GC()

		if err := pprof.WriteHeapProfile(f); err != nil {
			f.Close()
			log.Fatal("could not start MEM profile: ", err)
		}
	}
}
