package api

import (
	"net/http"

	"github.com/freeekanayaka/kvsql/server/db"
)

func New(localNodeAddress string, db *db.DB, changes chan *db.KeyValue, subscribe SubcribeFunc) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/dqlite", dqliteHandleFunc(localNodeAddress))
	mux.HandleFunc("/watch", watchHandleFunc(db, changes, subscribe))
	return mux
}
