module github.com/hanfei1991/microcosm

go 1.16

require (
	github.com/BurntSushi/toml v0.4.1
	github.com/gogo/protobuf v1.3.2
	github.com/pingcap/errors v0.11.5-0.20211009033009-93128226aaa3
	github.com/pingcap/failpoint v0.0.0-20210918120811-547c13e3eb00 // indirect
	github.com/pingcap/ticdc v0.0.0-20211122030349-23c0c6dbd8a8
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.0
	go.etcd.io/etcd v0.5.0-alpha.5.0.20210512015243-d19fbe541bf9
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	go.uber.org/goleak v1.1.11-0.20210813005559-691160354723
	go.uber.org/zap v1.19.1
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	google.golang.org/grpc v1.40.0
)

// cloud.google.com/go/storage will upgrade grpc to v1.40.0
// we need keep the replacement until go.etcd.io supports the higher version of grpc.
replace google.golang.org/grpc v1.40.0 => google.golang.org/grpc v1.29.1
