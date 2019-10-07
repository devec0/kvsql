package db

import (
	"context"

	"github.com/pkg/errors"
)

// MaxServerID returns the highest server ID.
func (d *DB) MaxServerID(ctx context.Context) (uint64, error) {
	stmt := "SELECT id FROM servers ORDER BY id DESC LIMIT 1"
	row := d.db.QueryRowContext(ctx, stmt)
	id := uint64(0)
	if err := row.Scan(&id); err != nil {
		return 0, errors.Wrap(err, "query max server ID")
	}
	return id, nil
}

func (d *DB) AddServer(ctx context.Context, id uint64, address string) error {
	stmt := "INSERT INTO servers(id, address) VALUES(?, ?)"
	if _, err := d.db.ExecContext(ctx, stmt, id, address); err != nil {
		return errors.Wrap(err, "insert new server")
	}
	return nil
}

type Server struct {
	ID      int64
	Address string
}

func (d *DB) GetServers(ctx context.Context) ([]Server, error) {
	servers := []Server{}
	rows, err := d.db.QueryContext(ctx, "SELECT id, address FROM servers")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		server := Server{}
		if err := rows.Scan(&server.ID, &server.Address); err != nil {
			return nil, err
		}
		servers = append(servers, server)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return servers, nil
}
