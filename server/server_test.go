package server_test

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/freeekanayaka/kvsql/server"
	"github.com/ghodss/yaml"
	"github.com/stretchr/testify/require"
)

func TestNew_FirstNode_Init(t *testing.T) {
	init := &server.Init{Address: "localhost:9999"}
	dir, cleanup := newDirWithInit(t, init)
	defer cleanup()

	server, err := server.New(dir)
	require.NoError(t, err)

	require.NoError(t, server.Close(context.Background()))
}

func TestNew_FirstNode_Restart(t *testing.T) {
	init := &server.Init{Address: "localhost:9999"}
	dir, cleanup := newDirWithInit(t, init)
	defer cleanup()

	s, err := server.New(dir)
	require.NoError(t, err)

	require.NoError(t, s.Close(context.Background()))

	s, err = server.New(dir)
	require.NoError(t, err)

	require.NoError(t, s.Close(context.Background()))
}

// Return a new temporary directory populated with the test cluster certificate
// and an init.yaml file with the given content.
func newDirWithInit(t *testing.T, init *server.Init) (string, func()) {
	dir, cleanup := newDirWithCert(t)

	path := filepath.Join(dir, "init.yaml")
	bytes, err := yaml.Marshal(init)
	require.NoError(t, err)
	require.NoError(t, ioutil.WriteFile(path, bytes, 0644))

	return dir, cleanup
}

// Return a new temporary directory populated with the test cluster certificate.
func newDirWithCert(t *testing.T) (string, func()) {
	t.Helper()

	dir, cleanup := newDir(t)

	// Create symlinks to the test certificates.
	for _, filename := range []string{"cluster.crt", "cluster.key"} {
		link := filepath.Join(dir, filename)
		target, err := filepath.Abs(filepath.Join("testdata", filename))
		require.NoError(t, err)
		require.NoError(t, os.Symlink(target, link))
	}

	return dir, cleanup
}

// Return a new temporary directory.
func newDir(t *testing.T) (string, func()) {
	t.Helper()

	dir, err := ioutil.TempDir("", "kvsql-server-test-")
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, os.RemoveAll(dir))
	}

	return dir, cleanup
}
