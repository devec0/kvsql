package server

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
	"github.com/freeekanayaka/kvsql/server/config"
	"github.com/freeekanayaka/kvsql/server/db"
	"github.com/freeekanayaka/kvsql/transport"
	"github.com/pkg/errors"
)

// Server sets up a single dqlite node and serves the cluster management API.
type Server struct {
	dir  string       // Data directory
	api  *http.Server // API server
	node *dqlite.Node // Dqlite node
	db   *db.DB       // Database connection
}

func New(dir string) (*Server, error) {
	// Check if we're initializing a new node (i.e. there's an init.yaml).
	cfg, err := config.Load(dir)
	if err != nil {
		return nil, err
	}

	// Create the dqlite dial function and driver now, we might need it below to join.
	driver, err := registerDriver(cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// It's safe to open the database object now, since no connection will
	// be attempted until we actually make use of it.
	db, err := db.Open(driver)
	if err != nil {
		return nil, errors.Wrap(err, "open cluster database")
	}

	// Possibly initialize our ID, address and initial node store content.
	if cfg.Init != nil {
		if err := initConfig(ctx, cfg, db); err != nil {
			return nil, err
		}
		if err := cfg.Save(dir); err != nil {
			return nil, err
		}
	}

	node, err := newNode(cfg, dir)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/dqlite", makeDqliteHandler(node.BindAddress()))
	api := &http.Server{Handler: mux}

	if err := startAPI(cfg, api); err != nil {
		return nil, err
	}

	// If we are initializing a new server, update the cluster state
	// accordingly.
	if cfg.Init != nil {
		if err := initServer(ctx, cfg, db); err != nil {
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

// Register a new Dqlite driver and return the registration name.
func registerDriver(cfg *config.Config) (string, error) {
	dial := dqliteDial(cfg.Cert)
	timeout := 10 * time.Second
	driver, err := driver.New(
		cfg.Store, driver.WithDialFunc(dial),
		driver.WithConnectionTimeout(timeout),
		driver.WithContextTimeout(timeout),
	)
	if err != nil {
		return "", errors.Wrap(err, "create dqlite driver")
	}

	// Create a unique name to pass to sql.Register.
	driverIndex++
	name := fmt.Sprintf("dqlite-%d", driverIndex)

	sql.Register(name, driver)

	return name, nil
}

var driverIndex = 0

// Initializes the configuration according to the content of the init.yaml
// file, possibly obtaining a new node ID.
func initConfig(ctx context.Context, cfg *config.Config, db *db.DB) error {
	servers := []client.NodeInfo{}

	if len(cfg.Init.Cluster) == 0 {
		servers = append(servers, client.NodeInfo{
			ID:      1,
			Address: cfg.Init.Address,
		})
	} else {
		for i, address := range cfg.Init.Cluster {
			servers = append(servers, client.NodeInfo{
				ID:      uint64(i + 1), // The ID isn't really used,
				Address: address,
			})
		}
	}

	if err := cfg.Store.Set(context.Background(), servers); err != nil {
		return errors.Wrap(err, "initialize node store")
	}

	if len(cfg.Init.Cluster) == 0 {
		cfg.ID = 1
	} else {
		// Figure out our ID.
		id, err := db.MaxServerID(ctx)
		if err != nil {
			return err
		}
		cfg.ID = id + 1
	}

	cfg.Address = cfg.Init.Address

	return nil
}

// Create a new dqlite node.
func newNode(cfg *config.Config, dir string) (*dqlite.Node, error) {
	// Wrap the regular dial function which one that also proxies the TLS
	// connection, since the raft connect function only supports Unix and
	// TCP connections.
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		dial := dqliteDial(cfg.Cert)
		tlsConn, err := dial(ctx, addr)
		if err != nil {
			return nil, err
		}
		goUnix, cUnix, err := transport.Socketpair()
		if err != nil {
			return nil, errors.Wrap(err, "create pair of Unix sockets")
		}

		transport.Proxy(goUnix, tlsConn)

		return cUnix, nil
	}

	node, err := dqlite.New(cfg.ID, cfg.Address, dir, dqlite.WithBindAddress("@"), dqlite.WithDialFunc(dial))
	if err != nil {
		return nil, errors.Wrap(err, "create dqlite node")
	}

	if err := node.Start(); err != nil {
		return nil, errors.Wrap(err, "start dqlite node")
	}

	return node, nil
}

// Create and start the server.
func startAPI(cfg *config.Config, api *http.Server) error {
	listener, err := transport.Listen(cfg.Address, cfg.Cert)
	if err != nil {
		return err
	}
	go func() {
		if err := api.Serve(listener); err != http.ErrServerClosed {
			panic(err)
		}
	}()
	return nil
}

func initServer(ctx context.Context, cfg *config.Config, db *db.DB) error {
	if len(cfg.Init.Cluster) == 0 {
		if err := db.CreateSchema(ctx); err != nil {
			return err
		}
	} else {
		if err := joinCluster(ctx, cfg); err != nil {
			return err
		}
	}
	if err := db.AddServer(ctx, cfg.ID, cfg.Address); err != nil {
		return err
	}
	return nil
}

// Make this node join an existing dqlite cluster.
func joinCluster(ctx context.Context, cfg *config.Config) error {
	dial := dqliteDial(cfg.Cert)
	info := client.NodeInfo{ID: cfg.ID, Address: cfg.Address}
	client, err := client.FindLeader(ctx, cfg.Store, client.WithDialFunc(dial))
	if err != nil {
		return errors.Wrap(err, "find leader")
	}
	defer client.Close()
	if err := client.Add(ctx, info); err != nil {
		return errors.Wrap(err, "join cluster")
	}
	return nil
}

func (s *Server) Close(ctx context.Context) error {
	if err := s.db.Close(); err != nil {
		return err
	}
	if err := s.api.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "shutdown API server")
	}
	if err := s.node.Close(); err != nil {
		return errors.Wrap(err, "stop dqlite node")
	}
	return nil
}
