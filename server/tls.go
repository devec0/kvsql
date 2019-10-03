package server

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"path/filepath"

	"github.com/pkg/errors"
)

// See https://github.com/denji/golang-tls
func newTLSServerConfig(dir string) (*tls.Config, error) {
	crt := filepath.Join(dir, "cluster.crt")
	key := filepath.Join(dir, "cluster.key")

	certificate, err := tls.LoadX509KeyPair(crt, key)
	if err != nil {
		return nil, errors.Wrap(err, "load cluster TLS certificate")
	}

	// Create a CA certificate pool and add cluster.crt to it
	data, err := ioutil.ReadFile(crt)
	if err != nil {
		return nil, errors.Wrap(err, "read cluster certificate")
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(data)

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
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	cfg.BuildNameToCertificate()

	return cfg, nil
}

// See https://venilnoronha.io/a-step-by-step-guide-to-mtls-in-go
func newTLSClientConfig(dir string) (*tls.Config, error) {
	crt := filepath.Join(dir, "cluster.crt")
	key := filepath.Join(dir, "cluster.key")

	certificate, err := tls.LoadX509KeyPair(crt, key)
	if err != nil {
		return nil, errors.Wrap(err, "load cluster TLS certificate")
	}

	// Create a CA certificate pool and add cluster.crt to it
	data, err := ioutil.ReadFile(crt)
	if err != nil {
		return nil, errors.Wrap(err, "read cluster certificate")
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(data)

	// Create a HTTPS client and supply the created CA pool
	cfg := &tls.Config{
		RootCAs:      pool,
		Certificates: []tls.Certificate{certificate},
	}
	return cfg, nil
}
