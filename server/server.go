package server

import (
	"context"
	"crypto/tls"
	"database/sql"
	"net"
	"net/http"
	"path/filepath"
	"time"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
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
	init, err := maybeLoadInit(dir)
	if err != nil {
		return nil, err
	}

	// Open the node store, effectively creating a new empty one if we're
	// initializing.
	store, err := client.DefaultNodeStore(filepath.Join(dir, "servers.sql"))
	if err != nil {
		return nil, errors.Wrap(err, "open node store")
	}

	// Create the dqlite dial function and driver now, we might need it below to join.
	dial, err := makeDqliteDialFunc(dir)
	if err != nil {
		return nil, err
	}
	driver, err := driver.New(
		store, driver.WithDialFunc(dial),
		driver.WithConnectionTimeout(10*time.Second),
		driver.WithContextTimeout(10*time.Second),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create dqlite driver")
	}
	name := makeDriverName()
	sql.Register(name, driver)

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

	cfg, err := newTLSServerConfig(dir)
	if err != nil {
		return nil, err
	}

	listener, err := tls.Listen("tcp", info.Address, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "bind API address")
	}

	node, err := dqlite.New(
		info.ID, info.Address, dir, dqlite.WithBindAddress("@"), dqlite.WithDialFunc(dial))
	if err != nil {
		return nil, errors.Wrap(err, "create dqlite node")
	}
	if err := node.Start(); err != nil {
		return nil, errors.Wrap(err, "start dqlite node")
	}

	conns := make(chan net.Conn)

	startDqliteProxy(conns, node.BindAddress())

	mux := http.NewServeMux()
	mux.HandleFunc("/dqlite", makeDqliteHandler(conns))
	api := &http.Server{Handler: mux}

	go func() {
		if err := api.Serve(listener); err != http.ErrServerClosed {
			panic(err)
		}
		close(conns)
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
			client, err := client.FindLeader(ctx, store, client.WithDialFunc(dial))
			if err != nil {
				return nil, errors.Wrap(err, "find leader")
			}
			defer client.Close()
			if err := client.Add(ctx, info); err != nil {
				return nil, errors.Wrap(err, "join cluster")
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
