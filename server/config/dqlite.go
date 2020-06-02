package config

import (
	"context"
	"os"
	"path/filepath"

	"github.com/canonical/go-dqlite/client"
	"github.com/pkg/errors"
)

// LoadNodeStore open the servers.sql SQLite database containing the addresses
// of the servers in the cluster.
func loadNodeStore(dir string) (client.NodeStore, error) {
	store, err := client.NewYamlNodeStore(filepath.Join(dir, "cluster.yaml"))
	if err != nil {
		return nil, errors.Wrap(err, "open node store")
	}

	// Possibly migrate from older path.
	legacyStorePath := filepath.Join(dir, "servers.sql")
	if _, err := os.Stat(legacyStorePath); err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	} else {
		legacyStore, err := client.DefaultNodeStore(legacyStorePath)
		if err != nil {
			return nil, errors.Wrap(err, "open legacy node store")
		}
		servers, err := legacyStore.Get(context.Background())
		if err != nil {
			return nil, errors.Wrap(err, "get servers from legacy node store")
		}
		if err := store.Set(context.Background(), servers); err != nil {
			return nil, errors.Wrap(err, "migrate servers to new node store")
		}
	}

	return store, nil
}
