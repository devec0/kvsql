package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/emicklei/go-restful"
	"github.com/freeekanayaka/kvsql/db"
)

func clusterHandler(db *db.DB) http.Handler {
	ws := new(restful.WebService)
	ws.Path("/cluster").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON)
	ws.Doc("dqlite cluster management")
	ws.Route(ws.GET("/").To(
		func(req *restful.Request, resp *restful.Response) {
			servers, err := db.GetServers(context.Background())
			if err != nil {
				msg := fmt.Sprintf("500 can't list servers: %v", err)
				http.Error(resp, msg, http.StatusServiceUnavailable)
				return
			}
			resp.WriteEntity(servers)
		}))

	c := restful.NewContainer()
	c.Add(ws)

	return c
}
