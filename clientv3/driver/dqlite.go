package driver

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strings"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	"github.com/ghodss/yaml"
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
	insertSQL = `
INSERT INTO key_value(` + fieldList + `)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	schema = []string{
		`create table if not exists key_value
			(
				name INTEGER,
				value BLOB,
				create_revision INTEGER,
				revision INTEGER,
				ttl INTEGER,
				version INTEGER,
				del INTEGER,
				old_value BLOB,
				id INTEGER primary key autoincrement,
				old_revision INTEGER
			)`,
		`create index if not exists name_idx on key_value (name)`,
		`create index if not exists revision_idx on key_value (revision)`,
	}
)

func newGeneric() *Generic {
	return &Generic{
		CleanupSQL:      "DELETE FROM key_value WHERE ttl > 0 AND ttl < ?",
		GetSQL:          "SELECT id, " + fieldList + " FROM key_value WHERE name = ? ORDER BY revision DESC limit ?",
		ListSQL:         strings.Replace(strings.Replace(baseList, "%REV%", "", -1), "%RES%", "", -1),
		ListRevisionSQL: strings.Replace(strings.Replace(baseList, "%REV%", "WHERE kvi.revision >= ?", -1), "%RES%", "", -1),
		ListResumeSQL: strings.Replace(strings.Replace(baseList, "%REV%", "WHERE kvi.revision <= ?", -1),
			"%RES%", "and kv.name > ? ", -1),
		InsertSQL:      insertSQL,
		ReplaySQL:      "SELECT id, " + fieldList + " FROM key_value WHERE name like ? and revision >= ? ORDER BY revision ASC",
		GetRevisionSQL: "SELECT MAX(revision) FROM key_value",
		ToDeleteSQL:    "SELECT count(*) c, name, max(revision) FROM key_value GROUP BY name HAVING c > 1 or (c = 1 and del = 1)",
		DeleteOldSQL:   "DELETE FROM key_value WHERE name = ? AND (revision < ? OR (revision = ? AND del = 1))",
	}
}

func NewDQLite(dir string) (*Generic, error) {
	infoPath := filepath.Join(dir, "info.yaml")
	if _, err := os.Stat(infoPath); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("no dqlite configuration found: please run 'kubectl dqlite bootstrap' or 'kubectl dqlite join'")
		}
		return nil, err

	}

	info := dqlite.ServerInfo{}
	data, err := ioutil.ReadFile(infoPath)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(data, &info)
	if err != nil {
		return nil, err
	}

	server, err := dqlite.NewServer(info, dir)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", info.Address)
	if err != nil {
		return nil, err
	}
	err = server.Start(listener)
	if err != nil {
		return nil, err
	}

	store, err := dqlite.DefaultServerStore(filepath.Join(dir, "servers.sql"))
	if err != nil {
		return nil, err
	}

	driver, err := dqlite.NewDriver(store)
	if err != nil {
		return nil, err
	}
	sql.Register("dqlite", driver)

	db, err := sql.Open("dqlite", "k8s")
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	for _, stmt := range schema {
		_, err := db.Exec(stmt)
		if err != nil {
			return nil, err
		}
	}

	g := newGeneric()
	g.db = db
	g.server = server

	return g, nil
}
