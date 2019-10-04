package db

import (
	"database/sql"
	"time"

	"github.com/pkg/errors"
)

type Notify func(*KeyValue) error

type DB struct {
	db     *sql.DB
	notify Notify
}

func Open(driver string, dsn string) (*DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, errors.Wrap(err, "open cluster database")
	}
	return &DB{db: db}, nil
}

// Notify sets a notification function that gets called whenever a key gets
// deleted or updated.
func (d *DB) Notify(notify Notify) {
	d.notify = notify
}

func (d *DB) Close() error {
	if err := d.db.Close(); err != nil {
		return errors.Wrap(err, "close cluster database")
	}
	return nil
}

// Executes the given function within a database transaction.
func (d *DB) tx(f func(*sql.Tx) error) error {
	return retry(func() error {
		tx, err := d.db.Begin()
		if err != nil {
			return errors.Wrap(err, "failed to begin transaction")
		}

		err = f(tx)
		if err != nil {
			return rollback(tx, err)
		}

		err = tx.Commit()
		if err == sql.ErrTxDone {
			err = nil // Ignore duplicate commits/rollbacks
		}
		return err
	})
}

// Retry transient db errors.
func retry(f func() error) error {
	var err error
	for i := 0; i < 10; i++ {
		if err = f(); err != nil {
			if err.Error() == "database is locked" {
				time.Sleep(250 * time.Millisecond)
				continue
			}
			return err
		}
		break
	}
	return err
}

// Rollback a transaction after the given error occurred. If the rollback
// succeeds the given error is returned, otherwise a new error that wraps it
// gets generated and returned.
func rollback(tx *sql.Tx, reason error) error {
	err := tx.Rollback()
	if err != nil {
		// TODO logger the error
	}
	return reason
}
