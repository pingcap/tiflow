module github.com/pingcap/ticdc

go 1.16

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/Shopify/sarama v1.27.2
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751
	github.com/apache/pulsar-client-go v0.6.0
	github.com/aws/aws-sdk-go v1.35.3
	github.com/bradleyjkemp/grpc-tools v0.2.5
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/chaos-mesh/go-sqlsmith v0.0.0-20211025024535-03ae33408684
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
	github.com/go-mysql-org/go-mysql v1.1.3-0.20210705101833-83965e516929
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gogo/gateway v1.1.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/google/btree v1.0.0
	github.com/google/go-cmp v0.5.6
	github.com/google/uuid v1.1.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.16.0
	github.com/integralist/go-findroot v0.0.0-20160518114804-ac90681525dc
	github.com/jarcoal/httpmock v1.0.5
	github.com/jmoiron/sqlx v1.3.3
	github.com/kami-zh/go-capturer v0.0.0-20171211120116-e492ea43421d
	github.com/labstack/echo/v4 v4.6.1
	github.com/lib/pq v1.3.0 // indirect
	github.com/linkedin/goavro/v2 v2.9.8
	github.com/mackerelio/go-osstat v0.1.0
	github.com/mattn/go-colorable v0.1.11 // indirect
	github.com/mattn/go-shellwords v1.0.12
	github.com/mattn/go-sqlite3 v2.0.2+incompatible // indirect
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20210513014640-40f9a1999b3b
	github.com/pingcap/failpoint v0.0.0-20210316064728-7acb0f0a3dfd
	github.com/pingcap/kvproto v0.0.0-20211011060348-d957056f1551
	github.com/pingcap/log v0.0.0-20210906054005-afc726e70354
	github.com/pingcap/tidb v1.1.0-beta.0.20211111080905-76b00f3ec11e
	github.com/pingcap/tidb-tools v5.2.2-0.20211019062242-37a8bef2fa17+incompatible
	github.com/pingcap/tidb/parser v0.0.0-20211111080905-76b00f3ec11e
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.7.1
	github.com/prometheus/client_model v0.2.0
	github.com/r3labs/diff v1.1.0
	github.com/rakyll/statik v0.1.7
	github.com/shopspring/decimal v1.3.0
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/swaggo/files v0.0.0-20190704085106-630677cd5c14
	github.com/swaggo/gin-swagger v1.2.0
	github.com/swaggo/swag v1.6.6-0.20200529100950-7c765ddd0476
	github.com/syndtr/goleveldb v1.0.1-0.20190318030020-c3a204f8e965
	github.com/tidwall/gjson v1.9.1
	github.com/tidwall/sjson v1.2.2
	github.com/tikv/client-go/v2 v2.0.0-alpha.0.20211029104011-2fd3841894de
	github.com/tikv/pd v1.1.0-beta.0.20211029083450-e65f0c55b6ae
	github.com/tinylib/msgp v1.1.0
	github.com/uber-go/atomic v1.4.0
	github.com/unrolled/render v1.0.1
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	go.etcd.io/etcd v0.5.0-alpha.5.0.20210512015243-d19fbe541bf9
	go.uber.org/atomic v1.9.0
	go.uber.org/goleak v1.1.11-0.20210813005559-691160354723
	go.uber.org/multierr v1.7.0
	go.uber.org/zap v1.19.1
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519 // indirect
	golang.org/x/net v0.0.0-20211020060615-d418f374d309
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20211025201205-69cdffdb9359
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
