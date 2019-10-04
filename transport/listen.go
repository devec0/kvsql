package transport

import (
	"crypto/tls"
	"net"

	"github.com/pkg/errors"
)

// Listen binds the given address and starts listening for incoming connections
// using the given TLS certificates.
func Listen(addr string, cert *Cert) (net.Listener, error) {
	// See https://github.com/denji/golang-tls
	cfg := &tls.Config{
		MinVersion:               tls.VersionTLS12,
		CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		},
		Certificates: []tls.Certificate{cert.KeyPair},
		ClientCAs:    cert.Pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	cfg.BuildNameToCertificate()

	listener, err := tls.Listen("tcp", addr, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "bind API address")
	}

	return listener, nil
}
