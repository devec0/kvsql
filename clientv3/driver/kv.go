package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/freeekanayaka/kvsql/db"
	"github.com/freeekanayaka/kvsql/transport"
	"github.com/pkg/errors"
)

func (d *Driver) List(ctx context.Context, revision, limit int64, rangeKey, startKey string) ([]*db.KeyValue, int64, error) {
	db := d.server.DB()
	return db.List(ctx, revision, limit, rangeKey, startKey)
}

func (d *Driver) Get(ctx context.Context, key string) (*db.KeyValue, error) {
	db := d.server.DB()
	return db.Get(ctx, key)
}

func (d *Driver) Update(ctx context.Context, key string, value []byte, revision, ttl int64) (*db.KeyValue, *db.KeyValue, error) {
	db := d.server.DB()
	kv, err := db.Mod(ctx, false, key, value, revision, ttl)
	if err != nil {
		return nil, nil, err
	}
	addr, err := d.server.Leader(ctx)
	if err != nil {
		return nil, nil, err
	}
	if err := postWatchChange(d.server.Cert(), addr, kv); err != nil {
		return nil, nil, err
	}

	if kv.Version == 1 {
		return nil, kv, nil
	}

	oldKv := *kv
	oldKv.Revision = oldKv.OldRevision
	oldKv.Value = oldKv.OldValue
	return &oldKv, kv, nil
}

func (d *Driver) Delete(ctx context.Context, key string, revision int64) ([]*db.KeyValue, error) {
	if strings.HasSuffix(key, "%") {
		panic("can not delete list revision")
	}
	db := d.server.DB()
	kv, err := db.Mod(ctx, true, key, []byte{}, revision, 0)
	if err != nil {
		return nil, err
	}
	addr, err := d.server.Leader(ctx)
	if err != nil {
		return nil, err
	}
	if err := postWatchChange(d.server.Cert(), addr, kv); err != nil {
		return nil, err
	}
	return nil, err
}

func postWatchChange(cert *transport.Cert, addr string, kv *db.KeyValue) error {
	url := fmt.Sprintf("http://%s/watch", addr)

	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(*kv)

	client := transport.HTTP(cert)

	response, err := client.Post(url, "application/json; charset=utf-8", b)
	if err != nil {
		return errors.Wrap(err, "Sending HTTP request failed")
	}

	if response.StatusCode != 200 {
		return fmt.Errorf("HTTP request failed with: %s", response.Status)
	}

	return nil
}
