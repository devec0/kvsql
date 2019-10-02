package server

import (
	"io/ioutil"
	"path/filepath"

	"github.com/canonical/go-dqlite"
	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
)

func writeInfo(dir string, info dqlite.NodeInfo) error {
	bytes, err := yaml.Marshal(info)
	if err != nil {
		return errors.Wrap(err, "encode server info")
	}

	path := filepath.Join(dir, "info.yaml")
	if err := ioutil.WriteFile(path, bytes, 0644); err != nil {
		return errors.Wrap(err, "write server info")
	}

	return nil
}
