module github.com/pingcap/tiflow

go 1.16

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/Shopify/sarama v1.27.2
	github.com/apache/pulsar-client-go v0.1.1
	github.com/benbjohnson/clock v1.3.0
	github.com/bradleyjkemp/grpc-tools v0.2.5
	github.com/cenkalti/backoff/v4 v4.0.2
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/coreos/go-semver v0.3.0
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/edwingeng/deque v0.0.0-20191220032131-8596380dee17
	github.com/fatih/color v1.10.0
	github.com/frankban/quicktest v1.11.1 // indirect
	github.com/go-sql-driver/mysql v1.5.0
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang/protobuf v1.3.4
	github.com/golang/snappy v0.0.2 // indirect
	github.com/google/btree v1.0.0
	github.com/google/go-cmp v0.5.4
	github.com/google/uuid v1.1.1
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.14.3 // indirect
	github.com/integralist/go-findroot v0.0.0-20160518114804-ac90681525dc
	github.com/jarcoal/httpmock v1.0.5
	github.com/jmoiron/sqlx v1.2.0
	github.com/klauspost/compress v1.11.1 // indirect
	github.com/lib/pq v1.3.0 // indirect
	github.com/linkedin/goavro/v2 v2.9.7
	github.com/mattn/go-shellwords v1.0.3
	github.com/mattn/go-sqlite3 v2.0.2+incompatible // indirect
	github.com/onsi/ginkgo v1.9.0 // indirect
	github.com/onsi/gomega v1.6.0 // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pingcap/br v5.0.0-nightly.0.20210419090151-03762465b589+incompatible
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pingcap/failpoint v0.0.0-20210316064728-7acb0f0a3dfd
	github.com/pingcap/kvproto v0.0.0-20210429093846-65f54a202d7e
	github.com/pingcap/log v0.0.0-20210317133921-96f4fcab92a4
	github.com/pingcap/parser v0.0.0-20210427084954-8e8ed7927bde
	github.com/pingcap/tidb v1.1.0-beta.0.20210508083641-8ed1d9d4a798
	github.com/pingcap/tidb-tools v4.0.9-0.20201127090955-2707c97b3853+incompatible
	github.com/prometheus/client_golang v1.5.1
	github.com/r3labs/diff v1.1.0
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.6.1
	github.com/tikv/pd v1.1.0-beta.0.20210323121136-78679e5e209d
	github.com/tinylib/msgp v1.1.0
	github.com/uber-go/atomic v1.4.0
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	go.etcd.io/bbolt v1.3.4 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200824191128-ae9734ed278b
	go.uber.org/zap v1.16.0
	golang.org/x/crypto v0.0.0-20201016220609-9e8e0b390897 // indirect
	golang.org/x/exp v0.0.0-20200513190911-00229845015e // indirect
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
	golang.org/x/net v0.0.0-20210316092652-d523dce5a7f4
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	golang.org/x/text v0.3.6
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	google.golang.org/grpc v1.27.1
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
	upper.io/db.v3 v3.7.1+incompatible
)

// Fix CVE-2020-26160.
replace github.com/dgrijalva/jwt-go v3.2.0+incompatible => github.com/golang-jwt/jwt v3.2.1+incompatible
