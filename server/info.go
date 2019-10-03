package server

import (
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/canonical/go-dqlite"
	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
)

func writeInfo(dir string, info dqlite.NodeInfo) error {
	data, err := yaml.Marshal(info)
	if err != nil {
		return errors.Wrap(err, "encode server info")
	}

	path := filepath.Join(dir, "info.yaml")
	if err := ioutil.WriteFile(path, data, 0644); err != nil {
		return errors.Wrap(err, "write server info")
	}

	return nil
}

func loadInfo(dir string, info *dqlite.NodeInfo) error {
	path := filepath.Join(dir, "info.yaml")

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return errors.Wrap(err, "read info.yaml")
	}

	if err := yaml.Unmarshal(data, info); err != nil {
		return errors.Wrap(err, "parse info.yaml")
	}

	if info.ID == 0 {
		return fmt.Errorf("server ID is zero")
	}
	if info.Address == "" {
		return fmt.Errorf("server address is empty")
	}

	return nil
}
