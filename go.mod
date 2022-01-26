module github.com/pingcap/tiflow

go 1.16

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/Shopify/sarama v1.27.2
	github.com/apache/pulsar-client-go v0.6.0
	github.com/aws/aws-sdk-go v1.35.3
	github.com/benbjohnson/clock v1.1.0
	github.com/bradleyjkemp/grpc-tools v0.2.5
	github.com/cenkalti/backoff/v4 v4.0.2
	github.com/chaos-mesh/go-sqlsmith v0.0.0-20211025024535-03ae33408684
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/cockroachdb/pebble v0.0.0-20210719141320-8c3bd06debb5
	github.com/coreos/go-semver v0.3.0
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/deepmap/oapi-codegen v1.9.0
	github.com/docker/go-units v0.4.0
	github.com/dustin/go-humanize v1.0.0
	github.com/edwingeng/deque v0.0.0-20191220032131-8596380dee17
	github.com/facebookgo/subset v0.0.0-20200203212716-c811ad88dec4
	github.com/fatih/color v1.10.0
	github.com/frankban/quicktest v1.11.1 // indirect
	github.com/getkin/kin-openapi v0.80.0
	github.com/gin-gonic/gin v1.7.4
	github.com/go-mysql-org/go-mysql v1.4.1-0.20211217061939-06f932768788
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gogo/gateway v1.1.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/google/btree v1.0.0
	github.com/google/go-cmp v0.5.6
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/google/uuid v1.1.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.16.0
	github.com/integralist/go-findroot v0.0.0-20160518114804-ac90681525dc
	github.com/jarcoal/httpmock v1.0.5
	github.com/jmoiron/sqlx v1.3.3
	github.com/kami-zh/go-capturer v0.0.0-20171211120116-e492ea43421d
	github.com/lib/pq v1.3.0 // indirect
	github.com/linkedin/goavro/v2 v2.9.8
	github.com/mattn/go-colorable v0.1.11 // indirect
	github.com/mattn/go-shellwords v1.0.12
	github.com/mattn/go-sqlite3 v2.0.2+incompatible // indirect
	github.com/modern-go/reflect2 v1.0.2
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/failpoint v0.0.0-20210918120811-547c13e3eb00
	github.com/pingcap/kvproto v0.0.0-20211207042851-78a55fb8e69c
	github.com/pingcap/log v0.0.0-20211207084639-71a2e5860834
	github.com/pingcap/tidb v1.1.0-beta.0.20220124083611-18fc286fbf0d
	github.com/pingcap/tidb-tools v5.2.3-0.20211105044302-2dabb6641a6e+incompatible
	github.com/pingcap/tidb/parser v0.0.0-20220124083611-18fc286fbf0d
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/client_model v0.2.0
	github.com/r3labs/diff v1.1.0
	github.com/shopspring/decimal v1.3.0
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cobra v1.2.1
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/swaggo/files v0.0.0-20190704085106-630677cd5c14
	github.com/swaggo/gin-swagger v1.2.0
	github.com/swaggo/swag v1.6.6-0.20200529100950-7c765ddd0476
	github.com/syndtr/goleveldb v1.0.1-0.20210305035536-64b5b1c73954
	github.com/tidwall/gjson v1.9.1
	github.com/tidwall/sjson v1.2.2
	github.com/tikv/client-go/v2 v2.0.0-rc.0.20211229051614-62d6b4a2e8f7
	github.com/tikv/pd v1.1.0-beta.0.20211118054146-02848d2660ee
	github.com/tinylib/msgp v1.1.0
	github.com/uber-go/atomic v1.4.0
	github.com/vmihailenco/msgpack/v5 v5.3.5
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	go.etcd.io/etcd v0.5.0-alpha.5.0.20210512015243-d19fbe541bf9
	go.uber.org/atomic v1.9.0
	go.uber.org/goleak v1.1.12
	go.uber.org/multierr v1.7.0
	go.uber.org/zap v1.19.1
	golang.org/x/net v0.0.0-20211020060615-d418f374d309
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20211216021012-1d35b9e2eb4e
	golang.org/x/text v0.3.7
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	google.golang.org/genproto v0.0.0-20210825212027-de86158e7fda
	google.golang.org/grpc v1.40.0
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22 // indirect
	gopkg.in/yaml.v2 v2.4.0
	sigs.k8s.io/yaml v1.2.0 // indirect
	upper.io/db.v3 v3.7.1+incompatible
)

replace (
	// Fix CVE-2020-26160.
	github.com/dgrijalva/jwt-go v3.2.0+incompatible => github.com/golang-jwt/jwt v3.2.1+incompatible
	// cloud.google.com/go/storage will upgrade grpc to v1.40.0
	// we need keep the replacement until go.etcd.io supports the higher version of grpc.
	google.golang.org/grpc v1.40.0 => google.golang.org/grpc v1.29.1
)
