package factory

import (
	"database/sql"
	"fmt"
	"net/http"

	restful "github.com/emicklei/go-restful"
	"github.com/freeekanayaka/kvsql/clientv3"
)

type Rest struct{}

type Server struct {
	ID      int64
	Address string
}

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
	servers, err := queryServers(db)
	if err != nil {
		msg := fmt.Sprintf("500 can't list servers: %v", err)
		http.Error(resp, msg, http.StatusServiceUnavailable)
		return
	}
	resp.WriteEntity(servers)
}

func queryServers(db *sql.DB) ([]Server, error) {
	servers := []Server{}
	rows, err := db.Query("SELECT id, address FROM servers")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		server := Server{}
		if err := rows.Scan(&server.ID, &server.Address); err != nil {
			return nil, err
		}
		servers = append(servers, server)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return servers, nil
}
