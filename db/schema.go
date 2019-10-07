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
	`CREATE TABLE key_value	(
           name INTEGER,
           value BLOB,
           create_revision INTEGER,
           revision INTEGER,
           ttl INTEGER,
           version INTEGER,
           del INTEGER,
           old_value BLOB,
           id INTEGER primary key autoincrement,
           old_revision INTEGER)`,
	`CREATE INDEX name_idx ON key_value (name)`,
	`CREATE INDEX revision_idx ON key_value (revision)`,
	`CREATE TABLE revision (
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           t TEXT)`,
	`INSERT INTO revision(t) VALUES(NULL)`, // Initial revision will be 1
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