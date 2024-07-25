//nolint:gochecknoinits,dogsled
package test

import (
	"os"
	"path"
	"runtime"
)

// ConfigTestRootPath - golang when running tests set the root folder to the folder
// of the file that it's being tested. This function change the root folder to the project
// root so that it's easier to reference file resources from the project root folder.
func ConfigTestRootPath() {
	_, filename, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(filename), "..")
	err := os.Chdir(dir)

	if err != nil {
		panic(err)
	}
}
