package db

import (
	"database/sql"

	"github.com/pkg/errors"
)

type DB struct {
	db *sql.DB
}

func Open(driver string, dsn string) (*DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, errors.Wrap(err, "open cluster database")
	}
	return &DB{db: db}, nil
}

func (d *DB) Close() error {
	if err := d.db.Close(); err != nil {
		return errors.Wrap(err, "close cluster database")
	}
	return nil
}
