package transport

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/pkg/errors"
)

// Dial establishes a secure connection with the given server.
func Dial(ctx context.Context, cert *Cert, addr string) (net.Conn, error) {
	deadline, _ := ctx.Deadline()
	dialer := &net.Dialer{Timeout: time.Until(deadline)}

	cfg := &tls.Config{
		RootCAs:      cert.Pool,
		Certificates: []tls.Certificate{cert.KeyPair},
	}

	conn, err := tls.DialWithDialer(dialer, "tcp", addr, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "connect to server")
	}

	return conn, nil
}
