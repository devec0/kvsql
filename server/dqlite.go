package server

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/canonical/go-dqlite/client"
	"github.com/pkg/errors"
)

func startDqliteProxy(conns chan net.Conn, addr string) *dqliteProxy {
	proxy := &dqliteProxy{conns: conns, addr: addr}
	go proxy.Start()
	return proxy
}

type dqliteProxy struct {
	conns chan net.Conn
	addr  string
}

func (p *dqliteProxy) Start() {
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

func makeDqliteDialFunc(dir string) (client.DialFunc, error) {
	cfg, err := newTLSClientConfig(dir)
	if err != nil {
		return nil, err
	}
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
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

		deadline, _ := ctx.Deadline()
		dialer := &net.Dialer{Timeout: time.Until(deadline)}

		conn, err := tls.DialWithDialer(dialer, "tcp", addr, cfg)
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

	return dial, nil
}
