package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"

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
