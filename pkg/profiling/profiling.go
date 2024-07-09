package profiling

import (
	"log"
	"os"
	"runtime"
	"runtime/pprof"
)

func StartCPUProfile(filename string) func() {
	f, err := os.Create(filename)
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

func CaptureMemoryProfile(filename string) {
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}
	defer f.Close()
	runtime.GC() // perform a garbage collection to get up-to-date statistics
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Fatal("could not write memory profile: ", err)
	}
}
