/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package factory

import (
	"context"
	"fmt"

	"github.com/freeekanayaka/kvsql/clientv3"
	etcd3 "github.com/freeekanayaka/kvsql/storage"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/value"
)

func NewKVSQLHealthCheck(c storagebackend.Config) (func() error, error) {
	// TODO: implement a reasonable health check for dqlite
	return func() error { return nil }, nil
}

func newETCD3Client(c storagebackend.Config) (*clientv3.Client, error) {
	if c.Dir == "" {
		return nil, fmt.Errorf("no storage directory provided")
	}

	cfg := clientv3.Config{
		Dir: c.Dir,
	}

	client, err := clientv3.New(cfg)
	return client, err
}

func NewKVSQLStorage(c storagebackend.Config) (storage.Interface, func(), error) {
	client, err := newETCD3Client(c)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	etcd3.StartCompactor(ctx, client, c.CompactionInterval)
	destroyFunc := func() {
		cancel()
		client.Close()
	}
	transformer := c.Transformer
	if transformer == nil {
		transformer = value.IdentityTransformer
	}

	return etcd3.New(client, c.Codec, c.Prefix, transformer, c.Paging), destroyFunc, nil
}

func Close() {
	clientv3.Shutdown()
}
