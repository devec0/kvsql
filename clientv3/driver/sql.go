package driver

import (
	"context"
	"database/sql"
	"strings"
)

var (
	fieldList = "name, value, old_value, old_revision, create_revision, revision, ttl, version, del"
	baseList  = `
SELECT kv.id, kv.name, kv.value, kv.old_value, kv.old_revision, kv.create_revision, kv.revision, kv.ttl, kv.version, kv.del
FROM key_value kv
  INNER JOIN
    (
      SELECT MAX(revision) revision, kvi.name
      FROM key_value kvi
		%REV%
        GROUP BY kvi.name
    ) AS r
    ON r.name = kv.name AND r.revision = kv.revision
WHERE kv.name like ? %RES% ORDER BY kv.name ASC limit ?
`

	cleanupSQL      = "DELETE FROM key_value WHERE ttl > 0 AND ttl < ?"
	getSQL          = "SELECT id, " + fieldList + " FROM key_value WHERE name = ? ORDER BY revision DESC limit ?"
	listSQL         = strings.Replace(strings.Replace(baseList, "%REV%", "", -1), "%RES%", "", -1)
	listRevisionSQL = strings.Replace(strings.Replace(baseList, "%REV%", "WHERE kvi.revision >= ?", -1), "%RES%", "", -1)
	listResumeSQL   = strings.Replace(strings.Replace(baseList, "%REV%", "WHERE kvi.revision <= ?", -1),
		"%RES%", "and kv.name > ? ", -1)
	insertSQL = `
INSERT INTO key_value(` + fieldList + `)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
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
