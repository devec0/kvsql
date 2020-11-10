module github.com/devec0/kvsql

go 1.14

replace (
	github.com/devec0/kine => github.com/chris-sanders/kine v0.5.2-0.20201110025446-96a1966c7f90
	github.com/rancher/kine => github.com/chris-sanders/kine v0.5.2-0.20201110025446-96a1966c7f90
)

require (
	github.com/canonical/go-dqlite v1.8.0
	github.com/devec0/kine v0.5.2
	github.com/emicklei/go-restful v2.13.0+incompatible
	github.com/ghodss/yaml v1.0.0
	github.com/go-sql-driver/mysql v1.5.0 // indirect
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/protobuf v1.4.2 // indirect
	github.com/google/go-cmp v0.5.0 // indirect
	github.com/json-iterator/go v1.1.10 // indirect
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.6.0
	go.etcd.io/etcd v0.0.0-20191023171146-3cf2f69b5738
	go.uber.org/zap v1.15.0 // indirect
	golang.org/x/net v0.0.0-20200202094626-16171245cfb2 // indirect
	golang.org/x/text v0.3.3 // indirect
	google.golang.org/grpc v1.26.0 // indirect
	gopkg.in/yaml.v2 v2.3.0
	k8s.io/apimachinery v0.17.0
	k8s.io/apiserver v0.17.0
	k8s.io/klog/v2 v2.4.0
)
