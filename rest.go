package factory

import (
	dqlite "github.com/CanonicalLtd/go-dqlite"
	restful "github.com/emicklei/go-restful"
)

type Rest struct{}

func (r Rest) Install(c *restful.Container) {
	ws := new(restful.WebService)
	ws.Path("/dqlite")
	ws.Doc("dqlite cluster management")
	ws.Route(ws.GET("/").To(getHandler))
	c.Add(ws)
}

func getHandler(req *restful.Request, resp *restful.Response) {
	info := dqlite.ServerInfo{
		ID:      1,
		Address: "1",
	}
	resp.WriteEntity(info)
}
