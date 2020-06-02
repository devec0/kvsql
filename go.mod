module github.com/freeekanayaka/kvsql

go 1.14

require (
	github.com/canonical/go-dqlite v1.5.2
	github.com/emicklei/go-restful v2.12.0+incompatible
	github.com/ghodss/yaml v1.0.0
	github.com/pkg/errors v0.9.1
	github.com/rancher/kine v0.0.0-00010101000000-000000000000
	github.com/sirupsen/logrus v1.6.0
)

replace (
	github.com/rancher/kine => /home/free/go/src/github.com/rancher/kine
	k8s.io/apiserver => github.com/freeekanayaka/apiserver v0.0.0-20200602093706-7bac4cbb4237
)
