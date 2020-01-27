package api

import (
	"net/http"

	"github.com/canonical/go-dqlite/client"
	"github.com/freeekanayaka/kvsql/db"
)

func New(localNodeAddress string, db *db.DB, store client.NodeStore, dial client.DialFunc, changes chan *db.KeyValue, subscribe SubcribeFunc) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/dqlite", dqliteHandleFunc(localNodeAddress))
	mux.HandleFunc("/watch", watchHandleFunc(db, changes, subscribe))
	mux.Handle("/cluster", clusterHandler(store, dial))
	return mux
}
