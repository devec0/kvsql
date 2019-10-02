package server

import (
	"context"
	"database/sql"

	"github.com/canonical/go-dqlite"
	"github.com/pkg/errors"
)

func createServersTable(ctx context.Context, db *sql.DB) error {
	stmt := `
CREATE TABLE servers (
  id INTEGER PRIMARY KEY NOT NULL,
  address TEXT NOT NULL,
  UNIQUE (address)
)`
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return errors.Wrap(err, "create servers table")
	}
	return nil
}

func insertServer(ctx context.Context, db *sql.DB, info dqlite.NodeInfo) error {
	stmt := "INSERT INTO servers(id, address) VALUES(?, ?)"
	if _, err := db.ExecContext(ctx, stmt, info.ID, info.Address); err != nil {
		return errors.Wrap(err, "insert new server")
	}
	return nil
}
