module github.com/pingcap/ticdc

go 1.16

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/Shopify/sarama v1.27.2
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751
	github.com/apache/pulsar-client-go v0.6.0
	github.com/bradleyjkemp/grpc-tools v0.2.5
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/chaos-mesh/go-sqlsmith v0.0.0-20210914111832-b3d328d69449
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/coreos/go-semver v0.3.0
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/deepmap/oapi-codegen v1.8.3
	github.com/docker/go-units v0.4.0
	github.com/dustin/go-humanize v1.0.0
	github.com/edwingeng/deque v0.0.0-20191220032131-8596380dee17
	github.com/fatih/color v1.10.0
	github.com/frankban/quicktest v1.11.1 // indirect
	github.com/getkin/kin-openapi v0.79.0
	github.com/gin-gonic/gin v1.7.0
	github.com/go-mysql-org/go-mysql v1.3.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gogo/gateway v1.1.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/google/btree v1.0.0
	github.com/google/go-cmp v0.5.6
	github.com/google/uuid v1.1.2
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.16.0
	github.com/integralist/go-findroot v0.0.0-20160518114804-ac90681525dc
	github.com/jarcoal/httpmock v1.0.5
	github.com/jmoiron/sqlx v1.3.3
	github.com/kami-zh/go-capturer v0.0.0-20171211120116-e492ea43421d
	github.com/labstack/echo/v4 v4.2.1
	github.com/lib/pq v1.3.0 // indirect
	github.com/linkedin/goavro/v2 v2.9.8
	github.com/mackerelio/go-osstat v0.1.0
	github.com/mattn/go-shellwords v1.0.3
	github.com/mattn/go-sqlite3 v2.0.2+incompatible // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/dumpling v0.0.0-20211014113126-e754a70f980c
	github.com/pingcap/errors v0.11.5-0.20210513014640-40f9a1999b3b
	github.com/pingcap/failpoint v0.0.0-20210316064728-7acb0f0a3dfd
	github.com/pingcap/kvproto v0.0.0-20210927053809-df38c15b57b3
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/parser v0.0.0-20210907051057-948434fa20e4
	github.com/pingcap/tidb v1.1.0-beta.0.20210915013841-0854595a6992
	github.com/pingcap/tidb-tools v5.2.0-alpha.0.20210727084616-915b22e4d42c+incompatible
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/client_model v0.2.0
	github.com/r3labs/diff v1.1.0
	github.com/rakyll/statik v0.1.7
	github.com/shopspring/decimal v1.3.0
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/swaggo/files v0.0.0-20190704085106-630677cd5c14
	github.com/swaggo/gin-swagger v1.2.0
	github.com/swaggo/swag v1.6.6-0.20200529100950-7c765ddd0476
	github.com/syndtr/goleveldb v1.0.1-0.20190318030020-c3a204f8e965
	github.com/tidwall/gjson v1.9.1
	github.com/tidwall/sjson v1.2.2
	github.com/tikv/client-go/v2 v2.0.0-alpha.0.20210913094925-a8fa8acd44e7
	github.com/tikv/pd v1.1.0-beta.0.20210818082359-acba1da0018d
	github.com/tinylib/msgp v1.1.0
	github.com/uber-go/atomic v1.4.0
	github.com/unrolled/render v1.0.1
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200824191128-ae9734ed278b
	go.uber.org/atomic v1.9.0
	go.uber.org/goleak v1.1.10
	go.uber.org/zap v1.19.0
	golang.org/x/net v0.0.0-20210503060351-7fd8e65b6420
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210806184541-e5e7981a1069
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
