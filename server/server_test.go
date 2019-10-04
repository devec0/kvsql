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
	init := &server.Init{Address: "localhost:9991"}
	dir, cleanup := newDirWithInit(t, init)
	defer cleanup()

	server, err := server.New(dir)
	require.NoError(t, err)

	require.NoError(t, server.Close(context.Background()))
}

func TestNew_FirstNode_Restart(t *testing.T) {
	init := &server.Init{Address: "localhost:9991"}
	dir, cleanup := newDirWithInit(t, init)
	defer cleanup()

	s, err := server.New(dir)
	require.NoError(t, err)

	require.NoError(t, s.Close(context.Background()))

	s, err = server.New(dir)
	require.NoError(t, err)

	require.NoError(t, s.Close(context.Background()))
}

func TestNew_SecondNode_Init(t *testing.T) {
	init1 := &server.Init{Address: "localhost:9991"}
	dir1, cleanup1 := newDirWithInit(t, init1)
	defer cleanup1()

	s1, err := server.New(dir1)
	require.NoError(t, err)

	init2 := &server.Init{Address: "localhost:9992", Cluster: []string{"localhost:9991"}}
	dir2, cleanup2 := newDirWithInit(t, init2)
	defer cleanup2()

	s2, err := server.New(dir2)
	require.NoError(t, err)

	require.NoError(t, s1.Close(context.Background()))
	require.NoError(t, s2.Close(context.Background()))
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