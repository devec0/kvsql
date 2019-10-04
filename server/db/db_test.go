package db_test

import (
	"context"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/freeekanayaka/kvsql/server/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	db, err := db.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

func TestCreateSchema(t *testing.T) {
	db, err := db.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	assert.NoError(t, db.CreateSchema(context.Background()))
}
