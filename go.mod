module github.com/freeekanayaka/kvsql

go 1.14

require (
	github.com/canonical/go-dqlite v1.5.2
	github.com/docker/docker v0.7.3-0.20190327010347-be7ac8be2ae0
	github.com/emicklei/go-restful v2.12.0+incompatible
	github.com/ghodss/yaml v1.0.0
	github.com/mattn/go-sqlite3 v1.10.0
	github.com/pkg/errors v0.9.1
	github.com/rancher/kine v0.4.0
	github.com/sirupsen/logrus v1.6.0
	github.com/stretchr/testify v1.4.0
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200401174654-e694b7bb0875
	golang.org/x/net v0.0.0-20200324143707-d3edc9973b7e
	gopkg.in/yaml.v2 v2.2.8
	k8s.io/apimachinery v0.17.0
	k8s.io/apiserver v0.17.0
	k8s.io/klog v1.0.0
	k8s.io/utils v0.0.0-20200414100711-2df71ebbae66
)

replace (
	github.com/rancher/kine => github.com/freeekanayaka/kine v0.3.6-0.20200602100608-3cebf3b11584
	k8s.io/apiserver => github.com/freeekanayaka/apiserver v0.0.0-20200602093706-7bac4cbb4237
)
