package client

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
	"github.com/freeekanayaka/kvsql/config"
	"github.com/freeekanayaka/kvsql/transport"
	"github.com/pkg/errors"
)

// DialFunc returns a dqlite dial function that will establish the connection
// using the target server's /dqlite HTTP endpoint.
func DialFunc(cert *config.Cert) client.DialFunc {
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

		tlsConn, err := transport.Dial(ctx, cert, addr)
		if err != nil {
			return nil, errors.Wrap(err, "connect to HTTP endpoint")
		}

		err = request.Write(tlsConn)
		if err != nil {
			return nil, errors.Wrap(err, "HTTP request failed")
		}

		response, err := http.ReadResponse(bufio.NewReader(tlsConn), request)
		if err != nil {
			return nil, errors.Wrap(err, "read response")
		}
		if response.StatusCode != http.StatusSwitchingProtocols {
			return nil, fmt.Errorf("expected status code 101 got %d", response.StatusCode)
		}
		if response.Header.Get("Upgrade") != "dqlite" {
			return nil, fmt.Errorf("missing or unexpected Upgrade header in response")
		}

		goUnix, cUnix, err := transport.Socketpair()
		if err != nil {
			return nil, errors.Wrap(err, "create pair of Unix sockets")
		}

		transport.Proxy(goUnix, tlsConn)

		return cUnix, nil
	}
}

// Register a new Dqlite driver and return the registration name.
func RegisterDriver(store client.NodeStore, dial client.DialFunc) (string, error) {
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

var driverIndex = 0
