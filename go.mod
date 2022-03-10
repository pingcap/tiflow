module github.com/hanfei1991/microcosm

go 1.16

require (
	github.com/BurntSushi/toml v0.4.1
	github.com/benbjohnson/clock v1.1.0
	github.com/edwingeng/deque v0.0.0-20191220032131-8596380dee17
	github.com/gavv/monotime v0.0.0-20190418164738-30dba4353424
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/google/uuid v1.1.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/pingcap/check v0.0.0-20211026125417-57bd13f7b5f0
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/log v0.0.0-20211215031037-e024ba4eb0ee
	github.com/pingcap/tidb v1.1.0-beta.0.20220303050146-f5a52909b6af
	github.com/pingcap/tidb-tools v5.4.1-0.20220302060346-b6d9cbe5dff1+incompatible
	github.com/pingcap/tiflow v0.0.0-20220308162507-e342c02e789c
	github.com/prometheus/client_golang v1.12.1
	github.com/sergi/go-diff v1.2.0
	github.com/spf13/cobra v1.2.1
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	go.etcd.io/etcd/api/v3 v3.5.2
	go.etcd.io/etcd/client/pkg/v3 v3.5.2
	go.etcd.io/etcd/client/v3 v3.5.2
	go.etcd.io/etcd/server/v3 v3.5.2
	go.uber.org/atomic v1.9.0
	go.uber.org/dig v1.13.0
	go.uber.org/goleak v1.1.12
	go.uber.org/zap v1.20.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	google.golang.org/grpc v1.43.0
)

// cloud.google.com/go/storage will upgrade grpc to v1.40.0
// we need keep the replacement until go.etcd.io supports the higher version of grpc.
// replace google.golang.org/grpc v1.43.0 => google.golang.org/grpc v1.29.1
