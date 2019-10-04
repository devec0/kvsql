package config

import (
	"context"
	"path/filepath"

	"github.com/canonical/go-dqlite/client"
	"github.com/pkg/errors"
)

// LoadNodeStore open the servers.sql SQLite database containing the addresses
// of the servers in the cluster.
func LoadNodeStore(dir string) (client.NodeStore, error) {
	store, err := client.DefaultNodeStore(filepath.Join(dir, "servers.sql"))
	if err != nil {
		return nil, errors.Wrap(err, "open node store")
	}
	return store, nil
}

// InitNodeStore initializes the node store according to the given
// configuration.
func InitNodeStore(init *Init, store client.NodeStore) error {
	servers := []client.NodeInfo{}
	if len(init.Cluster) == 0 {
		// This is the first node of a new cluster.
		servers = append(servers, client.NodeInfo{
			ID:      1,
			Address: init.Address,
		})
	} else {
		for i, address := range init.Cluster {
			servers = append(servers, client.NodeInfo{
				ID:      uint64(i + 1), // The ID isn't really used,
				Address: address,
			})
		}
	}
	if err := store.Set(context.Background(), servers); err != nil {
		return err
	}
	return nil
}
