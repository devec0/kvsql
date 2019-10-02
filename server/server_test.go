package server_test

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/freeekanayaka/kvsql/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_FirstNode(t *testing.T) {
	dir, cleanup := newDirWithCert(t)
	defer cleanup()

	server, err := server.New(dir)
	require.NoError(t, err)

	require.NoError(t, server.Close(context.Background()))
}

// Return a new temporary directory populated with the test cluster certificate.
func newDirWithCert(t *testing.T) (string, func()) {
	t.Helper()

	dir, cleanup := newDir(t)

	// Create symlinks to the test certificates.
	for _, filename := range []string{"cluster.crt", "cluster.key"} {
		link := filepath.Join(dir, filename)
		target, err := filepath.Abs(filepath.Join("testdata", filename))
		assert.NoError(t, err)
		assert.NoError(t, os.Symlink(target, link))
	}

	return dir, cleanup
}

// Return a new temporary directory.
func newDir(t *testing.T) (string, func()) {
	t.Helper()

	dir, err := ioutil.TempDir("", "kvsql-server-test-")
	assert.NoError(t, err)

	cleanup := func() {
		assert.NoError(t, os.RemoveAll(dir))
	}

	return dir, cleanup
}
