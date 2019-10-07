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
	// TODO: honor the given context's deadline
	// deadline, _ := ctx.Deadline()
	// timeout := time.Until(deadline)
	timeout := 5 * time.Second
	dialer := &net.Dialer{Timeout: timeout}

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
