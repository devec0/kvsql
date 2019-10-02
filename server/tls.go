package server

import (
	"crypto/tls"
	"path/filepath"

	"github.com/pkg/errors"
)

// See https://github.com/denji/golang-tls
func newTLSConfig(dir string) (*tls.Config, error) {
	crt := filepath.Join(dir, "cluster.crt")
	key := filepath.Join(dir, "cluster.key")

	certificate, err := tls.LoadX509KeyPair(crt, key)
	if err != nil {
		return nil, errors.Wrap(err, "load cluster TLS certificate")
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
		Certificates: []tls.Certificate{certificate},
	}

	return cfg, nil
}
