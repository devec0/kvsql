package factory

import (
	"fmt"
	"net/http"

	restful "github.com/emicklei/go-restful"
	"github.com/freeekanayaka/kvsql/clientv3"
	"github.com/freeekanayaka/kvsql/clientv3/driver"
)

type Rest struct{}

func (r Rest) Install(c *restful.Container) {
	ws := new(restful.WebService)
	ws.Path("/dqlite").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON)
	ws.Doc("dqlite cluster management")
	ws.Route(ws.GET("/").To(getHandler))
	c.Add(ws)
}

func getHandler(req *restful.Request, resp *restful.Response) {
	db := clientv3.DB()
	if db == nil {
		http.Error(resp, "503 dqlite engine not ready", http.StatusServiceUnavailable)
		return
	}
	servers, err := driver.QueryServers(db)
	if err != nil {
		msg := fmt.Sprintf("500 can't list servers: %v", err)
		http.Error(resp, msg, http.StatusServiceUnavailable)
		return
	}
	resp.WriteEntity(servers)
}
