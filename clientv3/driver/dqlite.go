package driver

import (
	"bufio"
	"bytes"
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
	"strconv"
	"strings"
	"time"

	dqlite "github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
)

var (
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
		`create table if not exists revision
                        (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                t TEXT
                        )`,
	}
)

func newGeneric() *Driver {
	return &Driver{
		ReplaySQL:      "SELECT id, " + fieldList + " FROM key_value WHERE name like ? and revision >= ? ORDER BY revision ASC",
		GetRevisionSQL: "SELECT id FROM revision",
		ToDeleteSQL:    "SELECT count(*) c, name, max(revision) FROM key_value GROUP BY name HAVING c > 1 or (c = 1 and del = 1)",
		DeleteOldSQL:   "DELETE FROM key_value WHERE name = ? AND (revision < ? OR (revision = ? AND del = 1))",
	}
}

func NewDQLite(dir string) (*Driver, error) {
	infoPath := filepath.Join(dir, "info.yaml")
	if _, err := os.Stat(infoPath); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("no dqlite configuration found: please run 'kubectl dqlite bootstrap' or 'kubectl dqlite join'")
		}
		return nil, err

	}

	info := client.NodeInfo{}
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

	g := newGeneric()

	mux := http.NewServeMux()
	mux.HandleFunc("/dqlite", makeDqliteHandler(conns))
	mux.HandleFunc("/watch", makeWatchHandler(g))

	web := &http.Server{Handler: mux}
	go web.Serve(listener)

	dial := makeDqliteDialFunc()
	server, err := dqlite.New(
		info.ID, info.Address, dir, dqlite.WithBindAddress("@"), dqlite.WithDialFunc(dial))
	if err != nil {
		return nil, err
	}

	proxy := &proxyListener{conns: conns, addr: server.BindAddress()}
	go proxy.Start()

	err = server.Start()
	if err != nil {
		return nil, err
	}

	store, err := client.DefaultNodeStore(filepath.Join(dir, "servers.sql"))
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
		client, err := client.FindLeader(ctx, store, client.WithDialFunc(dial))
		if err != nil {
			return nil, fmt.Errorf("can't find leader: %v", err)
		}
		defer client.Close()
		if err := client.Add(ctx, info); err != nil {
			return nil, fmt.Errorf("can't join: %v", err)
		}
		if err := os.Remove(filepath.Join(dir, "join")); err != nil {
			return nil, err
		}
		shouldInsertServer = true
	}

	driver, err := driver.New(
		store, driver.WithDialFunc(dial),
		driver.WithConnectionTimeout(10*time.Second),
		driver.WithContextTimeout(10*time.Second),
	)
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
		f := func() error {
			_, err := db.Exec(addServerSQL, info.ID, info.Address)
			return err
		}
		if err := retry(f); err != nil {
			return nil, fmt.Errorf("can't insert server: %v", err)
		}
	}

	g.db = db
	g.info = info
	g.server = server
	g.store = store

	return g, nil
}

type proxyListener struct {
	conns chan net.Conn
	addr  string
}

func (p *proxyListener) Start() {
	for {
		src, ok := <-p.conns
		if !ok {
			break
		}
		dst, err := net.Dial("unix", p.addr)
		if err != nil {
			continue
		}
		go func() {
			_, err := io.Copy(dst, src)
			if err != nil {
				fmt.Printf("Dqlite server proxy TLS -> Unix: %v\n", err)
			}
			src.Close()
			dst.Close()
		}()

		go func() {
			_, err := io.Copy(src, dst)
			if err != nil {
				fmt.Printf("Dqlite server proxy Unix -> TLS: %v\n", err)
			}
			src.Close()
			dst.Close()
		}()
	}
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

func makeWatchHandler(g *Driver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Receive change notifications.
		if r.Method == "POST" {
			kv := KeyValue{}
			if err := readToJSON(r.Body, &kv); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			g.changes <- &kv
		}

		// Broadcast change notifications.
		if r.Method == "GET" {
			if r.Header.Get("Upgrade") != "watch" {
				http.Error(w, "Missing or invalid upgrade header", http.StatusBadRequest)
				return
			}

			key := r.Header.Get("X-Watch-Key")
			if key == "" {
				http.Error(w, "Missing key header", http.StatusBadRequest)
				return
			}

			rev := r.Header.Get("X-Watch-Rev")
			if rev == "" {
				http.Error(w, "Missing rev header", http.StatusBadRequest)
				return
			}
			revision, err := strconv.Atoi(rev)
			if err != nil {
				http.Error(w, "Bad revision", http.StatusBadRequest)
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
			data := []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: watch\r\n\r\n")
			if n, err := conn.Write(data); err != nil || n != len(data) {
				conn.Close()
				return
			}

			prefix := strings.HasSuffix(key, "%")

			ctx, parentCancel := context.WithCancel(context.Background())

			defer func() {
				conn.Close()
				parentCancel()
			}()

			events, err := g.broadcaster.Subscribe(ctx, g.globalWatcher)
			if err != nil {
				panic(err)
			}

			writer := bufio.NewWriter(conn)

			if err := sendEvent(writer, &Event{Start: true}); err != nil {
				return
			}

			if revision > 0 {
				keys, err := g.replayEvents(ctx, key, int64(revision))
				if err != nil {
					return
				}

				for _, k := range keys {
					if err := sendEvent(writer, &Event{KV: k}); err != nil {
						return
					}
				}
			}

			for e := range events {
				k, ok := e["data"].(*KeyValue)
				if ok && matchesKey(prefix, key, k) {
					if err := sendEvent(writer, &Event{KV: k}); err != nil {
						return
					}
				}
			}

		}
	}
}

func sendEvent(writer *bufio.Writer, e *Event) error {
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(e)

	if _, err := writer.Write(b.Bytes()); err != nil {
		return err
	}

	if err := writer.Flush(); err != nil {
		return err
	}

	return nil
}

func makeDqliteDialFunc() client.DialFunc {
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
