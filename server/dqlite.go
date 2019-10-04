package server

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
	"github.com/freeekanayaka/kvsql/config"
	"github.com/freeekanayaka/kvsql/transport"
	"github.com/pkg/errors"
)

// Returns a dqlite dial function that will establish the connection
// using the target server's /dqlite HTTP endpoint.
func dqliteDial(cert *config.Cert) client.DialFunc {
	return func(ctx context.Context, addr string) (net.Conn, error) {
		request := &http.Request{
			Method:     "POST",
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header:     make(http.Header),
			Host:       addr,
		}
		path := fmt.Sprintf("https://%s/dqlite", addr)

		var err error
		request.URL, err = url.Parse(path)
		if err != nil {
			return nil, err
		}

		request.Header.Set("Upgrade", "dqlite")
		request = request.WithContext(ctx)

		conn, err := transport.Dial(ctx, cert, addr)
		if err != nil {
			return nil, errors.Wrap(err, "connect to HTTP endpoint")
		}

		err = request.Write(conn)
		if err != nil {
			return nil, errors.Wrap(err, "HTTP request failed")
		}

		response, err := http.ReadResponse(bufio.NewReader(conn), request)
		if err != nil {
			return nil, errors.Wrap(err, "read response")
		}
		if response.StatusCode != http.StatusSwitchingProtocols {
			return nil, fmt.Errorf("expected status code 101 got %d", response.StatusCode)
		}
		if response.Header.Get("Upgrade") != "dqlite" {
			return nil, fmt.Errorf("missing or unexpected Upgrade header in response")
		}

		return conn, nil
	}
}

// Register a new Dqlite driver and return the registration name.
func dqliteDriver(store client.NodeStore, cert *config.Cert) (string, error) {
	dial := dqliteDial(cert)
	timeout := 10 * time.Second
	driver, err := driver.New(
		store, driver.WithDialFunc(dial),
		driver.WithConnectionTimeout(timeout),
		driver.WithContextTimeout(timeout),
	)
	if err != nil {
		return "", errors.Wrap(err, "create dqlite driver")
	}

	// Create a unique name to pass to sql.Register.
	driverIndex++
	name := fmt.Sprintf("dqlite-%d", driverIndex)

	sql.Register(name, driver)

	return name, nil
}

// Create a new dqlite node.
func dqliteNode(id uint64, address string, dir string, cert *config.Cert) (*dqlite.Node, error) {
	// Wrap the regular dial function which one that also proxies the TLS
	// connection, since the raft connect function only supports Unix and
	// TCP connections.
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		tlsDial := dqliteDial(cert)
		tlsConn, err := tlsDial(ctx, addr)
		if err != nil {
			return nil, err
		}
		goUnix, cUnix, err := transport.Socketpair()
		if err != nil {
			return nil, errors.Wrap(err, "create pair of Unix sockets")
		}

		transport.Proxy(goUnix, tlsConn)

		return cUnix, nil
	}

	node, err := dqlite.New(id, address, dir, dqlite.WithBindAddress("@"), dqlite.WithDialFunc(dial))
	if err != nil {
		return nil, errors.Wrap(err, "create dqlite node")
	}

	if err := node.Start(); err != nil {
		return nil, errors.Wrap(err, "start dqlite node")
	}

	return node, nil
}

var driverIndex = 0

func makeDqliteHandler(addr string) http.HandlerFunc {
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

		tlsConn, _, err := hijacker.Hijack()
		if err != nil {
			message := errors.Wrap(err, "Failed to hijack connection").Error()
			http.Error(w, message, http.StatusInternalServerError)
			return
		}

		// Write the status line and upgrade header by hand since w.WriteHeader()
		// would fail after Hijack()
		data := []byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: dqlite\r\n\r\n")
		if n, err := tlsConn.Write(data); err != nil || n != len(data) {
			tlsConn.Close()
			return
		}

		unixConn, err := net.Dial("unix", addr)
		if err != nil {
			panic("dqlite node is not listening to the given Unix socket")
		}

		transport.Proxy(tlsConn, unixConn)
	}
}

func addNode(ctx context.Context, store client.NodeStore, dial client.DialFunc, id uint64, address string) error {
	info := client.NodeInfo{ID: id, Address: address}
	client, err := client.FindLeader(ctx, store, client.WithDialFunc(dial))
	if err != nil {
		return errors.Wrap(err, "find leader")
	}
	defer client.Close()
	if err := client.Add(ctx, info); err != nil {
		return errors.Wrap(err, "join cluster")
	}
	return nil
}
