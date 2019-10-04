package client

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
	"github.com/freeekanayaka/kvsql/config"
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

		conn, err := dial(ctx, cert, addr)
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

		listener, err := net.Listen("unix", "")
		if err != nil {
			return nil, errors.Wrap(err, "Failed to create unix listener")
		}

		goUnix, err := net.Dial("unix", listener.Addr().String())
		if err != nil {
			return nil, errors.Wrap(err, "Failed to connect to unix listener")
		}

		cUnix, err := listener.Accept()
		if err != nil {
			return nil, errors.Wrap(err, "Failed to connect to unix listener")
		}

		listener.Close()

		go func() {
			_, err := io.Copy(goUnix, conn)
			if err != nil {
				fmt.Printf("Dqlite client proxy TLS -> Unix: %v\n", err)
			}
			goUnix.Close()
			conn.Close()
		}()

		go func() {
			_, err := io.Copy(conn, goUnix)
			if err != nil {
				fmt.Printf("Dqlite client proxy Unix -> TLS: %v\n", err)
			}
			conn.Close()
			goUnix.Close()
		}()

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
