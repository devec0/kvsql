package server

import (
	"os"
	"path/filepath"
)

func rmInit(dir string) error {
	return os.Remove(filepath.Join(dir, "init.yaml"))
}
