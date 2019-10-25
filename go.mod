module github.com/pingcap/tidb-cdc

go 1.12

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.3.3
	github.com/biogo/store v0.0.0-20190426020002-884f370e325d
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/coreos/etcd v3.3.13+incompatible
	github.com/go-sql-driver/mysql v1.4.1
	github.com/google/uuid v1.1.1
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/pingcap/errors v0.11.4
	github.com/pingcap/kvproto v0.0.0-20191016091455-93317d9d9702
	github.com/pingcap/log v0.0.0-20191012051959-b742a5d432e9
	github.com/pingcap/parser v0.0.0-20191012071233-32876040fefb
	github.com/pingcap/pd v1.1.0-beta.0.20190923032047-5c648dc365e0
	github.com/pingcap/tidb v1.1.0-beta.0.20191017064824-e1ba309148ad
	github.com/pingcap/tidb-tools v2.1.3-0.20190321065848-1e8b48f5c168+incompatible
	github.com/remyoudompheng/bigfft v0.0.0-20190728182440-6a916e37a237 // indirect
	github.com/sirupsen/logrus v1.4.2 // indirect
	github.com/spf13/cobra v0.0.3
	go.uber.org/zap v1.10.0
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	google.golang.org/appengine v1.6.2 // indirect
	google.golang.org/grpc v1.23.0
)
