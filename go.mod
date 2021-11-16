module github.com/pingcap/ticdc

go 1.16

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/Shopify/sarama v1.27.2
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751
	github.com/apache/pulsar-client-go v0.1.1
	github.com/bradleyjkemp/grpc-tools v0.2.5
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/coreos/go-semver v0.3.0
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/edwingeng/deque v0.0.0-20191220032131-8596380dee17
	github.com/fatih/color v1.10.0
	github.com/frankban/quicktest v1.11.1 // indirect
	github.com/gin-gonic/gin v1.7.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang/protobuf v1.5.2
	github.com/google/btree v1.0.0
	github.com/google/go-cmp v0.5.6
	github.com/google/uuid v1.1.2
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.14.3 // indirect
	github.com/integralist/go-findroot v0.0.0-20160518114804-ac90681525dc
	github.com/jarcoal/httpmock v1.0.5
	github.com/jmoiron/sqlx v1.2.0
	github.com/lib/pq v1.3.0 // indirect
	github.com/linkedin/goavro/v2 v2.9.7
	github.com/mattn/go-shellwords v1.0.3
	github.com/mattn/go-sqlite3 v2.0.2+incompatible // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20210425183316-da1aaba5fb63
	github.com/pingcap/failpoint v0.0.0-20210316064728-7acb0f0a3dfd
	github.com/pingcap/kvproto v0.0.0-20210806074406-317f69fb54b4
	github.com/pingcap/log v0.0.0-20210818144256-6455d4a4c6f9
	github.com/pingcap/parser v0.0.0-20210831085004-b5390aa83f65
	github.com/pingcap/tidb v1.1.0-beta.0.20210907130457-cd8fb24c5f7e
	github.com/pingcap/tidb-tools v5.0.3+incompatible
	github.com/prometheus/client_golang v1.5.1
	github.com/r3labs/diff v1.1.0
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/swaggo/files v0.0.0-20190704085106-630677cd5c14
	github.com/swaggo/gin-swagger v1.2.0
	github.com/swaggo/swag v1.6.6-0.20200529100950-7c765ddd0476
	github.com/tikv/client-go/v2 v2.0.0-alpha.0.20210831090540-391fcd842dc8
	github.com/tikv/pd v1.1.0-beta.0.20210818112400-0c5667766690
	github.com/tinylib/msgp v1.1.0
	github.com/uber-go/atomic v1.4.0
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200824191128-ae9734ed278b
	go.uber.org/goleak v1.1.10
	go.uber.org/zap v1.19.0
	golang.org/x/crypto v0.0.0-20201016220609-9e8e0b390897 // indirect
	golang.org/x/net v0.0.0-20210503060351-7fd8e65b6420
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/text v0.3.7
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	google.golang.org/grpc v1.29.1
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
	upper.io/db.v3 v3.7.1+incompatible
)

// Fix CVE-2020-26160.
replace github.com/dgrijalva/jwt-go v3.2.0+incompatible => github.com/golang-jwt/jwt v3.2.1+incompatible
