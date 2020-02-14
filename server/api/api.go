package api

import (
	"net/http"

	"github.com/freeekanayaka/kvsql/db"
	"github.com/freeekanayaka/kvsql/server/membership"
)

func New(localNodeAddress string, membership *membership.Membership, changes chan *db.KeyValue, subscribe SubcribeFunc) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/dqlite", dqliteHandleFunc(localNodeAddress))
	//mux.HandleFunc("/watch", watchHandleFunc(db, changes, subscribe))
	mux.Handle("/cluster", clusterHandler(membership))
	return mux
}
