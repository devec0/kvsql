package driver

import (
	"context"

	"github.com/freeekanayaka/kvsql/server"
)

type Driver struct {
	server *server.Server
}

func New(server *server.Server) *Driver {
	driver := &Driver{
		server: server,
	}
	return driver
}

func (d *Driver) WaitStopped() {
	d.server.Close(context.Background())
}
