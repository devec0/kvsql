package driver

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	dqlite "github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/client"
	"github.com/freeekanayaka/kvsql/pkg/broadcast"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Driver struct {
	db     *sql.DB
	info   client.NodeInfo
	server *dqlite.Node
	store  client.NodeStore

	GetSQL          string
	ListSQL         string
	ListRevisionSQL string
	ListResumeSQL   string
	ReplaySQL       string
	InsertSQL       string
	GetRevisionSQL  string
	ToDeleteSQL     string
	DeleteOldSQL    string

	changes     chan *KeyValue
	broadcaster broadcast.Broadcaster
	cancel      func()
	stopped     chan struct{}
}

func (g *Driver) DB() *sql.DB {
	return g.db
}

func (g *Driver) currentRevision(ctx context.Context) (int64, error) {
	row := g.db.QueryRowContext(ctx, g.GetRevisionSQL)
	rev := sql.NullInt64{}
	if err := row.Scan(&rev); err != nil && err != sql.ErrNoRows {
		return 0, errors.Wrap(err, "Failed to get initial revision")
	}
	if rev.Int64 == 0 {
		var err error
		rev.Int64, err = g.newRevision(ctx)
		if err != nil {
			return 0, errors.Wrap(err, "Failed to create initial revision")
		}
	}
	return rev.Int64, nil
}

func (g *Driver) newRevision(ctx context.Context) (int64, error) {
	tx, err := g.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM revision"); err != nil {
		return 0, err
	}
	result, err := tx.ExecContext(ctx, "INSERT INTO revision(t) VALUES(NULL)")
	if err != nil {
		return 0, err
	}
	revision, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return revision, nil
}

func (g *Driver) Start(ctx context.Context) error {
	g.changes = make(chan *KeyValue, 1024)
	g.stopped = make(chan struct{})

	if _, err := g.currentRevision(ctx); err != nil {
		return errors.Wrap(err, "Failed to initialize revision")
	}

	go func() {
		for {
			select {
			case <-time.After(5 * time.Second):
				g.updateServerStore()
			case <-ctx.Done():
				g.db.Close()
				g.server.Close()
				close(g.stopped)
				return
			case <-time.After(time.Minute):
				_, err := g.exec(ctx, cleanupSQL, time.Now().Unix())
				if err != nil {
					logrus.Errorf("Failed to purge expired TTL entries")
				}

				err = g.cleanup(ctx)
				if err != nil {
					logrus.Errorf("Failed to cleanup duplicate entries: %v", err)
				}
			}
		}
	}()

	return nil
}

func (g *Driver) updateServerStore() {
	servers, err := QueryServers(g.db)
	if err != nil {
		return
	}
	infos := make([]client.NodeInfo, len(servers))
	for i, server := range servers {
		infos[i].Address = server.Address
	}
	g.store.Set(context.Background(), infos)
}

func (g *Driver) WaitStopped() {
	<-g.stopped
}

func (g *Driver) cleanup(ctx context.Context) error {
	rows, err := g.query(ctx, g.ToDeleteSQL)
	if err != nil {
		return err
	}
	defer rows.Close()

	toDelete := map[string]int64{}
	for rows.Next() {
		var (
			count, revision int64
			name            string
		)
		err := rows.Scan(&count, &name, &revision)
		if err != nil {
			return err
		}
		toDelete[name] = revision
	}

	rows.Close()

	for name, rev := range toDelete {
		_, err = g.exec(ctx, g.DeleteOldSQL, name, rev, rev)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *Driver) replayEvents(ctx context.Context, key string, revision int64) ([]*KeyValue, error) {
	rows, err := g.query(ctx, g.ReplaySQL, key, revision)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var resp []*KeyValue
	for rows.Next() {
		value := KeyValue{}
		if err := scan(rows.Scan, &value); err != nil {
			return nil, err
		}
		resp = append(resp, &value)
	}

	return resp, nil
}

func postWatchChange(addr string, kv *KeyValue) error {
	url := fmt.Sprintf("http://%s/watch", addr)

	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(*kv)

	response, err := http.Post(url, "application/json; charset=utf-8", b)
	if err != nil {
		return errors.Wrap(err, "Sending HTTP request failed")
	}

	if response.StatusCode != 200 {
		return fmt.Errorf("HTTP request failed with: %s", response.Status)
	}

	return nil
}

type scanner func(dest ...interface{}) error

func scan(s scanner, out *KeyValue) error {
	return s(
		&out.ID,
		&out.Key,
		&out.Value,
		&out.OldValue,
		&out.OldRevision,
		&out.CreateRevision,
		&out.Revision,
		&out.TTL,
		&out.Version,
		&out.Del)
}

type Server struct {
	ID      int64
	Address string
}

func QueryServers(db *sql.DB) ([]Server, error) {
	servers := []Server{}
	rows, err := db.Query("SELECT id, address FROM servers")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		server := Server{}
		if err := rows.Scan(&server.ID, &server.Address); err != nil {
			return nil, err
		}
		servers = append(servers, server)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return servers, nil
}
