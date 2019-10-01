package driver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/canonical/go-dqlite/client"
	"github.com/pkg/errors"
)

func (g *Driver) List(ctx context.Context, revision, limit int64, rangeKey, startKey string) ([]*KeyValue, int64, error) {
	var (
		rows *sql.Rows
		err  error
	)

	if limit == 0 {
		limit = 1000000
	} else {
		limit = limit + 1
	}

	listRevision, err := g.currentRevision(ctx)
	if err != nil {
		return nil, 0, err
	}
	if !strings.HasSuffix(rangeKey, "%") && revision <= 0 {
		rows, err = g.query(ctx, getSQL, rangeKey, 1)
	} else if revision <= 0 {
		rows, err = g.query(ctx, listSQL, rangeKey, limit)
	} else if len(startKey) > 0 {
		listRevision = revision
		rows, err = g.query(ctx, g.ListResumeSQL, revision, rangeKey, startKey, limit)
	} else {
		rows, err = g.query(ctx, g.ListRevisionSQL, revision, rangeKey, limit)
	}

	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var resp []*KeyValue
	for rows.Next() {
		value := KeyValue{}
		if err := scan(rows.Scan, &value); err != nil {
			return nil, 0, err
		}
		if value.Revision > listRevision {
			listRevision = value.Revision
		}
		if value.Del == 0 {
			resp = append(resp, &value)
		}
	}

	return resp, listRevision, nil
}

func (g *Driver) Get(ctx context.Context, key string) (*KeyValue, error) {
	kvs, _, err := g.List(ctx, 0, 1, key, "")
	if err != nil {
		return nil, err
	}
	if len(kvs) > 0 {
		return kvs[0], nil
	}
	return nil, nil
}

func (g *Driver) Update(ctx context.Context, key string, value []byte, revision, ttl int64) (*KeyValue, *KeyValue, error) {
	kv, err := g.mod(ctx, false, key, value, revision, ttl)
	if err != nil {
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

func (g *Driver) Delete(ctx context.Context, key string, revision int64) ([]*KeyValue, error) {
	if strings.HasSuffix(key, "%") {
		panic("can not delete list revision")
	}

	_, err := g.mod(ctx, true, key, []byte{}, revision, 0)
	return nil, err
}

func (g *Driver) mod(ctx context.Context, delete bool, key string, value []byte, revision int64, ttl int64) (*KeyValue, error) {
	oldKv, err := g.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if revision > 0 && oldKv == nil {
		return nil, ErrNotExists
	}

	if revision > 0 && oldKv.Revision != revision {
		return nil, ErrRevisionMatch
	}

	if ttl > 0 {
		ttl = int64(time.Now().Unix()) + ttl
	}

	newRevision, err := g.newRevision(ctx)
	if err != nil {
		return nil, err
	}
	result := &KeyValue{
		Key:            key,
		Value:          value,
		Revision:       newRevision,
		TTL:            int64(ttl),
		CreateRevision: newRevision,
		Version:        1,
	}
	if oldKv != nil {
		result.OldRevision = oldKv.Revision
		result.OldValue = oldKv.Value
		result.TTL = oldKv.TTL
		result.CreateRevision = oldKv.CreateRevision
		result.Version = oldKv.Version + 1
	}

	if delete {
		result.Del = 1
	}

	_, err = g.exec(ctx, g.InsertSQL,
		result.Key,
		result.Value,
		result.OldValue,
		result.OldRevision,
		result.CreateRevision,
		result.Revision,
		result.TTL,
		result.Version,
		result.Del,
	)
	if err != nil {
		return nil, err
	}

	client, err := client.New(ctx, g.server.BindAddress())
	if err != nil {
		return nil, errors.Wrap(err, "create dqlite client")
	}
	defer client.Close()

	info, err := client.Leader(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "get leader")
	}
	if info == nil {
		return nil, fmt.Errorf("no leader found")
	}

	if err := postWatchChange(info.Address, result); err != nil {
		return nil, err
	}

	return result, nil
}
