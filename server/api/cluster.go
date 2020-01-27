package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/canonical/go-dqlite/client"
	"github.com/emicklei/go-restful"
)

func clusterHandler(store client.NodeStore, dial client.DialFunc) http.Handler {
	ws := new(restful.WebService)
	ws.Path("/cluster").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON)
	ws.Doc("dqlite cluster management")
	ws.Route(ws.GET("/").To(
		func(req *restful.Request, resp *restful.Response) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := client.FindLeader(ctx, store, client.WithDialFunc(dial))
			if err != nil {
				msg := fmt.Sprintf("500 can't connect to leader: %v", err)
				http.Error(resp, msg, http.StatusServiceUnavailable)
				return
			}
			defer client.Close()
			servers, err := client.Cluster(ctx)
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
