package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/pkg/errors"
)

// LoadTLS loads the cluster TLS configuration.
func LoadTLS(dir string) (tls.Certificate, *x509.CertPool, error) {
	crt := filepath.Join(dir, "cluster.crt")
	key := filepath.Join(dir, "cluster.key")

	keypair, err := tls.LoadX509KeyPair(crt, key)
	if err != nil {
		return tls.Certificate{}, nil, errors.Wrap(err, "load keypair")
	}

	data, err := ioutil.ReadFile(crt)
	if err != nil {
		return tls.Certificate{}, nil, errors.Wrap(err, "read certificate")
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return tls.Certificate{}, nil, fmt.Errorf("bad certificate")
	}

	return keypair, pool, nil
}
