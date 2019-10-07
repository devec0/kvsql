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
