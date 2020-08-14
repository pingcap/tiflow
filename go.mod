module github.com/pingcap/ticdc

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.3.3 // indirect
	github.com/Shopify/sarama v1.26.1
	github.com/apache/pulsar-client-go v0.1.1
	github.com/aws/aws-sdk-go v1.30.24
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
	github.com/jarcoal/httpmock v1.0.5
	github.com/linkedin/goavro/v2 v2.9.7
	github.com/mattn/go-shellwords v1.0.3
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/pingcap/br v0.0.0-20200805121136-181c081ba6ac
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20200729012136-4e113ddee29e
	github.com/pingcap/failpoint v0.0.0-20200603062251-b230c36c413c
	github.com/pingcap/kvproto v0.0.0-20200803054707-ebd5de15093f
	github.com/pingcap/log v0.0.0-20200511115504-543df19646ad
	github.com/pingcap/parser v0.0.0-20200731033026-84f62115187c
	github.com/pingcap/pd/v4 v4.0.0-rc.2.0.20200730093003-dc8c75cf7ca0
	github.com/pingcap/tidb v1.1.0-beta.0.20200810064414-d81150394f9d
	github.com/pingcap/tidb-tools v4.0.5-0.20200731060919-6a36d482e3f9+incompatible
	github.com/prometheus/client_golang v1.5.1
	github.com/r3labs/diff v1.1.0
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.5.1
	github.com/uber-go/atomic v1.3.2
	github.com/vmihailenco/msgpack/v5 v5.0.0-beta.1
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200425165423-262c93980547
	go.uber.org/zap v1.15.0
	golang.org/x/crypto v0.0.0-20200302210943-78000ba7a073 // indirect
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	golang.org/x/text v0.3.3
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	google.golang.org/genproto v0.0.0-20200113173426-e1de0a7b01eb // indirect
	google.golang.org/grpc v1.26.0
)

replace github.com/pingcap/br => github.com/3pointer/br v0.0.0-20200814072551-d2e93b5f05f4
