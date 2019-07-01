package driver

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	dqlite "github.com/CanonicalLtd/go-dqlite"
	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
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

	addServerSQL = `
INSERT INTO servers(id, address)
   VALUES(?, ?)`

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
		`create table if not exists servers
                        (
                                id INTEGER PRIMARY KEY NOT NULL,
                                address TEXT NOT NULL,
                                UNIQUE (address)
                        )`,
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

	listener, err := net.Listen("tcp", info.Address)
	if err != nil {
		return nil, err
	}
	conns := make(chan net.Conn)

	mux := http.NewServeMux()
	mux.HandleFunc("/dqlite", makeDqliteHandler(conns))
	mux.HandleFunc("/watch", makeWatchHandler())

	web := &http.Server{Handler: mux}
	go web.Serve(listener)

	dial := makeDqliteDialFunc()
	server, err := dqlite.NewServer(info, dir, dqlite.WithServerDialFunc(dial))
	if err != nil {
		return nil, err
	}

	proxy := &proxyListener{conns: conns, addr: listener.Addr()}
	err = server.Start(proxy)
	if err != nil {
		return nil, err
	}

	store, err := dqlite.DefaultServerStore(filepath.Join(dir, "servers.sql"))
	if err != nil {
		return nil, err
	}

	shouldInsertServer := false

	// Possibly insert the first server.
	if _, err := os.Stat(filepath.Join(dir, "bootstrap")); err == nil {
		if err := os.Remove(filepath.Join(dir, "bootstrap")); err != nil {
			return nil, err
		}
		shouldInsertServer = true
	}

	// Possibly join a cluster.
	if _, err := os.Stat(filepath.Join(dir, "join")); err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Join(ctx, store, dial); err != nil {
			return nil, fmt.Errorf("can't join: %v", err)
		}
		if err := os.Remove(filepath.Join(dir, "join")); err != nil {
			return nil, err
		}
		shouldInsertServer = true
	}

	driver, err := dqlite.NewDriver(store, dqlite.WithDialFunc(dial))
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

	// Possibly insert the first server.
	if shouldInsertServer {
		if _, err := db.Exec(addServerSQL, info.ID, info.Address); err != nil {
			return nil, fmt.Errorf("can't insert server: %v", err)
		}
	}

	g := newGeneric()
	g.db = db
	g.info = info
	g.server = server
	g.store = store

	return g, nil
}

type proxyListener struct {
	conns chan net.Conn
	addr  net.Addr
}

func (p *proxyListener) Accept() (net.Conn, error) {
	return <-p.conns, nil
}

func (p *proxyListener) Close() error {
	return nil
}

func (p *proxyListener) Addr() net.Addr {
	return p.addr
}

func makeDqliteHandler(conns chan net.Conn) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Upgrade") != "dqlite" {
			http.Error(w, "Missing or invalid upgrade header", http.StatusBadRequest)
			return
		}

		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "Webserver doesn't support hijacking", http.StatusInternalServerError)
			return
		}

		conn, _, err := hijacker.Hijack()
		if err != nil {
			message := errors.Wrap(err, "Failed to hijack connection").Error()
			http.Error(w, message, http.StatusInternalServerError)
			return
		}

		// Write the status line and upgrade header by hand since w.WriteHeader()
		// would fail after Hijack()
		data := []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: dqlite\r\n\r\n")
		if n, err := conn.Write(data); err != nil || n != len(data) {
			conn.Close()
			return
		}

		conns <- conn
	}
}

func readToJSON(r io.Reader, obj interface{}) error {
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	return json.Unmarshal(buf, obj)
}

func makeWatchHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Handle change notifications.
		if r.Method == "POST" {
			kv := KeyValue{}
			if err := readToJSON(r.Body, &kv); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}
}

func makeDqliteDialFunc() dqlite.DialFunc {
	return func(ctx context.Context, addr string) (net.Conn, error) {
		request := &http.Request{
			Method:     "POST",
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header:     make(http.Header),
			Host:       addr,
		}
		path := fmt.Sprintf("http://%s/dqlite", addr)

		var err error
		request.URL, err = url.Parse(path)
		if err != nil {
			return nil, err
		}

		request.Header.Set("Upgrade", "dqlite")
		request = request.WithContext(ctx)

		deadline, _ := ctx.Deadline()
		dialer := &net.Dialer{Timeout: time.Until(deadline)}

		conn, err := dialer.Dial("tcp", addr)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to connect to HTTP endpoint")
		}

		err = request.Write(conn)
		if err != nil {
			return nil, errors.Wrap(err, "Sending HTTP request failed")
		}

		response, err := http.ReadResponse(bufio.NewReader(conn), request)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to read response")
		}
		if response.StatusCode != http.StatusSwitchingProtocols {
			return nil, fmt.Errorf("Dialing failed: expected status code 101 got %d", response.StatusCode)
		}
		if response.Header.Get("Upgrade") != "dqlite" {
			return nil, fmt.Errorf("Missing or unexpected Upgrade header in response")
		}

		return conn, nil
	}
}
