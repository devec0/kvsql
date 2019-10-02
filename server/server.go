package server

import (
	"context"
	"crypto/tls"
	"net/http"

	"github.com/pkg/errors"
)

// Server sets up a single dqlite node and serves the cluster management API.
type Server struct {
	dir string       // Data directory
	api *http.Server // API server
}

func New(dir string) (*Server, error) {
	addr := ":9999"
	mux := http.NewServeMux()
	api := &http.Server{Addr: addr, Handler: mux}

	cfg, err := newTLSConfig(dir)
	if err != nil {
		return nil, err
	}

	listener, err := tls.Listen("tcp", addr, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "listen to API address")
	}

	go func() {
		if err := api.Serve(listener); err != http.ErrServerClosed {
			panic(err)
		}

	}()

	s := &Server{
		dir: dir,
		api: api,
	}

	return s, nil
}

func (s *Server) Close(ctx context.Context) error {
	if err := s.api.Shutdown(ctx); err != nil {
		return errors.Wrap(err, "shutdown API server")
	}
	return nil
}
