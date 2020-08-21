module github.com/pingcap/ticdc

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.3.3 // indirect
	github.com/Shopify/sarama v1.26.1
	github.com/apache/pulsar-client-go v0.1.1
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/coreos/go-semver v0.3.0
	github.com/davecgh/go-spew v1.1.1
	github.com/edwingeng/deque v0.0.0-20191220032131-8596380dee17
	github.com/go-sql-driver/mysql v1.5.0
	github.com/golang/protobuf v1.3.4
	github.com/google/btree v1.0.0
	github.com/google/uuid v1.1.1
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/integralist/go-findroot v0.0.0-20160518114804-ac90681525dc
	github.com/jarcoal/httpmock v1.0.5
	github.com/jmoiron/sqlx v1.2.0
	github.com/linkedin/goavro/v2 v2.9.7
	github.com/mattn/go-shellwords v1.0.3
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/pingcap/br v0.0.0-20200805095214-09dcc7534821
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/pingcap/kvproto v0.0.0-20200821062356-15dc121d6c4d
	github.com/pingcap/log v0.0.0-20200511115504-543df19646ad
	github.com/pingcap/parser v0.0.0-20200803072748-fdf66528323d
	github.com/pingcap/pd/v4 v4.0.5-0.20200817114353-e465cafe8a91
	github.com/pingcap/tidb v1.1.0-beta.0.20200820092836-c5b7658b0896
	github.com/pingcap/tidb-tools v4.0.5-0.20200817064459-ba61a7376547+incompatible
	github.com/pingcap/tipb v0.0.0-20200618092958-4fad48b4c8c3 // indirect
	github.com/prometheus/client_golang v1.5.1
	github.com/r3labs/diff v1.1.0
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.5.1
	github.com/tikv/pd v1.1.0-beta.0.20200821075321-0d566cbde39f // indirect
	github.com/uber-go/atomic v1.3.2
	github.com/uber/jaeger-lib v2.2.0+incompatible // indirect
	github.com/vmihailenco/msgpack/v5 v5.0.0-beta.1
	go.etcd.io/bbolt v1.3.5 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200425165423-262c93980547
	go.uber.org/zap v1.15.0
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	golang.org/x/text v0.3.3
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	google.golang.org/genproto v0.0.0-20200113173426-e1de0a7b01eb // indirect
	google.golang.org/grpc v1.26.0
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22 // indirect
	upper.io/db.v3 v3.7.1+incompatible
)

replace github.com/pingcap/br => github.com/3pointer/br v0.0.0-20200817122845-968a9b5582e8

replace github.com/pingcap/pd/v4 => github.com/tikv/pd v1.1.0-beta.0.20200821075321-0d566cbde39f
