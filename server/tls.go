package server

import (
	"crypto/tls"

	"github.com/freeekanayaka/kvsql/config"
	"github.com/pkg/errors"
)

// See https://github.com/denji/golang-tls
func newTLSServerConfig(dir string) (*tls.Config, error) {
	keypair, pool, err := config.LoadTLS(dir)
	if err != nil {
		return nil, errors.Wrap(err, "load TLS configuration")
	}

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
		Certificates: []tls.Certificate{keypair},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	cfg.BuildNameToCertificate()

	return cfg, nil
}

// See https://venilnoronha.io/a-step-by-step-guide-to-mtls-in-go
func newTLSClientConfig(dir string) (*tls.Config, error) {
	keypair, pool, err := config.LoadTLS(dir)
	if err != nil {
		return nil, errors.Wrap(err, "load TLS configuration")
	}

	// Create a HTTPS client and supply the created CA pool
	cfg := &tls.Config{
		RootCAs:      pool,
		Certificates: []tls.Certificate{keypair},
	}
	return cfg, nil
}
