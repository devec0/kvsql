package db

import (
	"context"

	"github.com/pkg/errors"
)

var schema = []string{
	`CREATE TABLE servers (
           id INTEGER PRIMARY KEY NOT NULL,
           address TEXT NOT NULL,
          UNIQUE (address))`,
}

// CreateSchema initializes the database schema.
func (d *DB) CreateSchema(ctx context.Context) error {
	for _, stmt := range schema {
		if _, err := d.db.ExecContext(ctx, stmt); err != nil {
			return errors.Wrap(err, "create schema")
		}
	}
	return nil
}
