module github.com/hanfei1991/microcosom

go 1.16

require (
	github.com/BurntSushi/toml v0.4.1
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gogo/protobuf v1.3.2
	github.com/juju/errors v0.0.0-20210818161939-5560c4c073ff // indirect
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20210425183316-da1aaba5fb63
	github.com/pingcap/failpoint v0.0.0-20210918120811-547c13e3eb00
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/tidb v2.0.11+incompatible
	github.com/pingcap/tidb-tools v5.0.3+incompatible
	github.com/pkg/errors v0.9.1
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200824191128-ae9734ed278b
	go.uber.org/zap v1.19.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/grpc v1.26.0
	google.golang.org/protobuf v1.27.1 // indirect
)

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.3
