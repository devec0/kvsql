package driver

import (
	"context"
	"database/sql"
)

const (
	fieldList = "name, value, old_value, old_revision, create_revision, revision, ttl, version, del"

	cleanupSQL = "DELETE FROM key_value WHERE ttl > 0 AND ttl < ?"
	getSQL     = "SELECT id, " + fieldList + " FROM key_value WHERE name = ? ORDER BY revision DESC limit ?"
)

func (g *Driver) query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	//trace := utiltrace.New(fmt.Sprintf("SQL DB QueryContext query: %s keys: %v", query, args))
	//defer trace.LogIfLong(500 * time.Millisecond)

	var err error
	var rows *sql.Rows
	f := func() error {
		rows, err = g.db.QueryContext(ctx, query, args...)
		return err
	}

	if err := retry(f); err != nil {
		return nil, err
	}

	return rows, nil
}

func (g *Driver) exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	//trace := utiltrace.New(fmt.Sprintf("SQL DB ExecContext query: %s keys: %v", query, args))
	//defer trace.LogIfLong(500 * time.Millisecond)

	var err error
	var result sql.Result
	f := func() error {
		result, err = g.db.ExecContext(ctx, query, args...)
		return err
	}

	if err := retry(f); err != nil {
		return nil, err
	}

	return result, nil
}
