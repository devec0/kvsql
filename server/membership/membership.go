package membership

import (
	"context"
	"time"

	"github.com/canonical/go-dqlite/client"
)

// Membership manages dqlite cluster membership.
type Membership struct {
	store client.NodeStore
	dial  client.DialFunc
}

func New(store client.NodeStore, dial client.DialFunc) *Membership {
	return &Membership{
		store: store,
		dial:  dial,
	}
}

func (m *Membership) List() ([]client.NodeInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	leader, err := m.getLeader()
	if err != nil {
		return nil, err
	}
	defer leader.Close()
	return leader.Cluster(ctx)
}

func (m *Membership) getLeader() (*client.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return client.FindLeader(ctx, m.store, client.WithDialFunc(m.dial))
}
