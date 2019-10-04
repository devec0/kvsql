package server

import (
	"crypto/tls"

	"github.com/freeekanayaka/kvsql/config"
)

// See https://github.com/denji/golang-tls
func newTLSServerConfig(cert *config.Cert) *tls.Config {
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

	return cfg
}
