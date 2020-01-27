package membership

import (
	"context"
	"time"

	"github.com/canonical/go-dqlite/client"
)

// Membership manages dqlite cluster membership.
type Membership struct {
	address string
	store   client.NodeStore
	dial    client.DialFunc
}

func New(address string, store client.NodeStore, dial client.DialFunc) *Membership {
	return &Membership{
		address: address,
		store:   store,
		dial:    dial,
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return client.FindLeader(ctx, m.store, client.WithDialFunc(m.dial))
}

// Best effort to shutdown gracefully.
func (m *Membership) Shutdown() {
	leader, err := m.getLeader()
	if err != nil {
		return
	}
	defer leader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info, err := leader.Leader(ctx)
	if err != nil {
		return
	}

	if info.Address != m.address {
		return
	}

	leader.Transfer(ctx, 0)
}
