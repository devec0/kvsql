package server

import (
	"context"
	"database/sql"
	"fmt"

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

// Return the highest server ID.
func queryMaxServerID(ctx context.Context, db *sql.DB) (uint64, error) {
	stmt := "SELECT id FROM servers ORDER BY id DESC LIMIT 1"
	row := db.QueryRowContext(ctx, stmt)
	id := uint64(0)
	if err := row.Scan(&id); err != nil {
		return 0, errors.Wrap(err, "query max server ID")
	}
	return id, nil
}

// Create a unique name to pass to sql.Register.
func makeDriverName() string {
	driverIndex++
	return fmt.Sprintf("dqlite-%d", driverIndex)
}

var driverIndex = 0
