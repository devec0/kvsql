package db_test

import (
	"context"
	"testing"

	"github.com/freeekanayaka/kvsql/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetServers(t *testing.T) {
	db, err := db.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	require.NoError(t, db.CreateSchema(ctx))
	require.NoError(t, db.AddServer(ctx, 1, "1"))

	servers, err := db.GetServers(ctx)
	require.NoError(t, err)

	assert.Len(t, servers, 1)
	assert.Equal(t, servers[0].ID, int64(1))
	assert.Equal(t, servers[0].Address, "1")
}
