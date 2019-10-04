package server

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/freeekanayaka/kvsql/server/config"
	"github.com/freeekanayaka/kvsql/transport"
	"github.com/pkg/errors"
)

// Server sets up a single dqlite node and serves the cluster management API.
type Server struct {
	dir  string       // Data directory
	api  *http.Server // API server
	node *dqlite.Node // Dqlite node
	db   *sql.DB      // Database connection
}

func New(dir string) (*Server, error) {
	// Check if we're initializing a new node (i.e. there's an init.yaml).
	init, err := config.LoadInit(dir)
	if err != nil {
		return nil, err
	}

	// Open the node store, effectively creating a new empty one if we're
	// initializing.
	store, err := config.LoadNodeStore(dir)
	if err != nil {
		return nil, err
	}

	// Load the TLS certificates.
	cert, err := transport.LoadCert(dir)
	if err != nil {
		return nil, err
	}

	// Create the dqlite dial function and driver now, we might need it below to join.
	name, err := dqliteDriver(store, cert)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info := dqlite.NodeInfo{}
	if init != nil {
		info.Address = init.Address
		if len(init.Cluster) == 0 {
			// This is the first node of a new cluster.
			info.ID = 1
			if err := store.Set(context.Background(), []client.NodeInfo{info}); err != nil {
				return nil, errors.Wrap(err, "initialize node store")
			}
		} else {
			servers := make([]client.NodeInfo, len(init.Cluster))
			for i, address := range init.Cluster {
				servers[i].ID = uint64(i + 1) // The ID isn't really used
				servers[i].Address = address
			}
			if err := store.Set(context.Background(), servers); err != nil {
				return nil, errors.Wrap(err, "initialize node store")
			}
			// Figure out our ID.
			db, err := sql.Open(name, "k8s")
			if err != nil {
				return nil, errors.Wrap(err, "open cluster database")
			}
			id, err := queryMaxServerID(ctx, db)
			if err != nil {
				return nil, err
			}
			info.ID = id + 1
		}
		if err := writeInfo(dir, info); err != nil {
			return nil, err
		}
		if err := rmInit(dir); err != nil {
			return nil, err
		}
	} else {
		if err := loadInfo(dir, &info); err != nil {
			return nil, err
		}
	}

	listener, err := transport.Listen(info.Address, cert)
	if err != nil {
		return nil, err
	}

	node, err := dqliteNode(info.ID, info.Address, dir, cert)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/dqlite", makeDqliteHandler(node.BindAddress()))
	api := &http.Server{Handler: mux}

	go func() {
		if err := api.Serve(listener); err != http.ErrServerClosed {
			panic(err)
		}
	}()

	db, err := sql.Open(name, "k8s")
	if err != nil {
		return nil, errors.Wrap(err, "open cluster database")
	}

	// If we are initializing a new node, update the cluster state
	// accordingly.
	if init != nil {
		if len(init.Cluster) == 0 {
			if err := createServersTable(ctx, db); err != nil {
				return nil, err
			}
		} else {
			if err := dqliteAdd(ctx, info.ID, info.Address, store, cert); err != nil {
				return nil, err
			}
		}
		if err := insertServer(ctx, db, info); err != nil {
			return nil, err
		}
	}

	s := &Server{
		dir:  dir,
		api:  api,
		node: node,
		db:   db,
	}

	return s, nil
}

func (s *Server) DB() *sql.DB {
	return s.db
}

func (s *Server) Close(ctx context.Context) error {
	if err := s.db.Close(); err != nil {
		return errors.Wrap(err, "close cluster database")
	}
	if err := s.api.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "shutdown API server")
	}
	if err := s.node.Close(); err != nil {
		return errors.Wrap(err, "stop dqlite node")
	}
	return nil
}
