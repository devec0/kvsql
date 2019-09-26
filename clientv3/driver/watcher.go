package driver

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/canonical/go-dqlite/client"
	"github.com/pkg/errors"
)

type Event struct {
	KV    *KeyValue
	Err   error
	Start bool
}

func matchesKey(prefix bool, key string, kv *KeyValue) bool {
	if kv == nil {
		return false
	}
	if prefix {
		return strings.HasPrefix(kv.Key, key[:len(key)-1])
	}
	return kv.Key == key
}

func (g *Generic) globalWatcher() (chan map[string]interface{}, error) {
	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel
	result := make(chan map[string]interface{}, 100)

	go func() {
		defer close(result)
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-g.changes:
				result <- map[string]interface{}{
					"data": e,
				}
			}
		}
	}()

	return result, nil
}

func (g *Generic) Watch(ctx context.Context, key string, revision int64) <-chan Event {
	ctx, parentCancel := context.WithCancel(ctx)

	watchChan := make(chan Event)
	go func() (returnErr error) {
		defer func() {
			sendErrorAndClose(watchChan, returnErr)
			parentCancel()
		}()
		ctx := context.Background()
		client, err := client.New(ctx, g.server.BindAddress())
		if err != nil {
			returnErr = errors.Wrap(err, "create dqlite client")
			return
		}

		info, err := client.Leader(ctx)
		if err != nil {
			returnErr = errors.Wrap(err, "get leader")
			return
		}

		if info == nil {
			returnErr = fmt.Errorf("no leader found")
			return
		}
		addr := info.Address

		request := &http.Request{
			Method:     "GET",
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header:     make(http.Header),
			Host:       addr,
		}
		path := fmt.Sprintf("http://%s/watch", addr)

		request.URL, returnErr = url.Parse(path)
		if returnErr != nil {
			return
		}

		request.Header.Set("Upgrade", "watch")
		request.Header.Set("X-Watch-Key", key)
		request.Header.Set("X-Watch-Rev", fmt.Sprintf("%d", revision))
		request = request.WithContext(ctx)

		dialer := &net.Dialer{}
		conn, err := dialer.Dial("tcp", addr)
		if err != nil {
			returnErr = err
			return
		}
		defer conn.Close()

		if returnErr = request.Write(conn); returnErr != nil {
			return
		}

		response, err := http.ReadResponse(bufio.NewReader(conn), request)
		if err != nil {
			returnErr = err
			return
		}
		if response.StatusCode != http.StatusSwitchingProtocols {
			returnErr = fmt.Errorf("Dialing failed: expected status code 101 got %d", response.StatusCode)
			return
		}
		if response.Header.Get("Upgrade") != "watch" {
			returnErr = fmt.Errorf("Missing or unexpected Upgrade header in response")
			return
		}

		reader := bufio.NewReader(conn)
		for {
			b, err := reader.ReadBytes('\n')
			if err != nil {
				returnErr = err
				return
			}
			e := Event{}
			if returnErr = json.Unmarshal(b, &e); returnErr != nil {
				return
			}
			watchChan <- e
		}

		return nil
	}()

	return watchChan
}

func start(watchResponses chan Event) {
	watchResponses <- Event{
		Start: true,
	}
}

func sendErrorAndClose(watchResponses chan Event, err error) {
	if err != nil {
		watchResponses <- Event{Err: err}
	}
	close(watchResponses)
}

// Close closes the watcher and cancels all watch requests.
func (g *Generic) Close() error {
	if g.cancel != nil {
		g.cancel()
		g.cancel = nil
	}
	return nil
}
