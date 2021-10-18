module github.com/pingcap/ticdc

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.3.3
	github.com/Shopify/sarama v1.27.2
	github.com/apache/pulsar-client-go v0.1.1
	github.com/bradleyjkemp/grpc-tools v0.2.5
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/coreos/go-semver v0.3.0
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
	github.com/integralist/go-findroot v0.0.0-20160518114804-ac90681525dc
	github.com/jarcoal/httpmock v1.0.5
	github.com/jmoiron/sqlx v1.2.0
	github.com/klauspost/compress v1.11.1 // indirect
	github.com/linkedin/goavro/v2 v2.9.7
	github.com/mackerelio/go-osstat v0.1.0
	github.com/mattn/go-runewidth v0.0.10 // indirect
	github.com/mattn/go-shellwords v1.0.3
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pingcap/br v4.0.11-0.20210119023619-139df44843ab+incompatible
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20201126102027-b0a155152ca3
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/pingcap/kvproto v0.0.0-20210308075244-560097d1309b
	github.com/pingcap/log v0.0.0-20201112100606-8f1e84a3abc8
	github.com/pingcap/parser v0.0.0-20210328063857-e44ba053d8bb
	github.com/pingcap/tidb v1.1.0-beta.0.20210512055339-e25d1d0b7354
	github.com/pingcap/tidb-tools v4.0.9-0.20201127090955-2707c97b3853+incompatible
	github.com/prometheus/client_golang v1.5.1
	github.com/r3labs/diff v1.1.0
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/shirou/gopsutil v3.20.12+incompatible // indirect
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.6.1
	github.com/tikv/pd v1.1.0-beta.0.20210105112549-e5be7fd38659
	github.com/tinylib/msgp v1.1.2
	github.com/uber-go/atomic v1.4.0
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200824191128-ae9734ed278b
	go.uber.org/zap v1.16.0
	golang.org/x/crypto v0.0.0-20201016220609-9e8e0b390897 // indirect
	golang.org/x/mod v0.4.1 // indirect
	golang.org/x/net v0.0.0-20210119194325-5f4716e94777
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	golang.org/x/sys v0.0.0-20210124154548-22da62e12c0c // indirect
	golang.org/x/text v0.3.5
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	golang.org/x/tools v0.1.0 // indirect
	google.golang.org/genproto v0.0.0-20200113173426-e1de0a7b01eb // indirect
	google.golang.org/grpc v1.26.0
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	upper.io/db.v3 v3.7.1+incompatible
)

// Fix CVE-2020-26160.
replace github.com/dgrijalva/jwt-go v3.2.0+incompatible => github.com/golang-jwt/jwt v3.2.1+incompatible
