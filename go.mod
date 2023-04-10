module github.com/pingcap/tiflow

go 1.18

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
	github.com/cockroachdb/pebble v0.0.0-20211124172904-3ca75111760c
	github.com/coreos/go-semver v0.3.0
	github.com/davecgh/go-spew v1.1.1
	github.com/deepmap/oapi-codegen v1.9.0
	github.com/docker/go-units v0.4.0
	github.com/dustin/go-humanize v1.0.0
	github.com/edwingeng/deque v0.0.0-20191220032131-8596380dee17
<<<<<<< HEAD
	github.com/facebookgo/subset v0.0.0-20200203212716-c811ad88dec4
	github.com/fatih/color v1.10.0
=======
	github.com/fatih/color v1.15.0
	github.com/gavv/monotime v0.0.0-20190418164738-30dba4353424
>>>>>>> 4ab802a50e (ddl(ticdc): add charset and collate to ddl event (#8723))
	github.com/getkin/kin-openapi v0.80.0
	github.com/gin-gonic/gin v1.7.4
	github.com/go-mysql-org/go-mysql v1.4.1-0.20220221114137-89145541e0d4
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gogo/gateway v1.1.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2
	github.com/google/btree v1.0.1
	github.com/google/go-cmp v0.5.6
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510
	github.com/google/uuid v1.1.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/grpc-ecosystem/grpc-gateway v1.16.0
	github.com/integralist/go-findroot v0.0.0-20160518114804-ac90681525dc
	github.com/jarcoal/httpmock v1.0.8
	github.com/jmoiron/sqlx v1.3.3
	github.com/kami-zh/go-capturer v0.0.0-20171211120116-e492ea43421d
	github.com/linkedin/goavro/v2 v2.9.8
	github.com/mattn/go-shellwords v1.0.12
	github.com/modern-go/reflect2 v1.0.2
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/pingcap/check v0.0.0-20211026125417-57bd13f7b5f0
<<<<<<< HEAD
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/pingcap/failpoint v0.0.0-20220303073211-00fea37feb66
	github.com/pingcap/kvproto v0.0.0-20220303025110-d5eb908517b2
	github.com/pingcap/log v0.0.0-20211215031037-e024ba4eb0ee
	github.com/pingcap/tidb v1.1.0-beta.0.20220309003348-b9bd5a7d7e4d
	github.com/pingcap/tidb-tools v6.0.0-alpha.0.20220317013353-dfc5146f4746+incompatible
	github.com/pingcap/tidb/parser v0.0.0-20220316172753-46c43febcac0
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/client_model v0.2.0
	github.com/r3labs/diff v1.1.0
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0
=======
	github.com/pingcap/errors v0.11.5-0.20221009092201-b66cddb77c32
	github.com/pingcap/failpoint v0.0.0-20220801062533-2eaa32854a6c
	github.com/pingcap/kvproto v0.0.0-20230317010544-b47a4830141f
	github.com/pingcap/log v1.1.1-0.20230317032135-a0d097d16e22
	github.com/pingcap/tidb v1.1.0-beta.0.20230406094457-915b39b8845a
	github.com/pingcap/tidb-tools v6.5.1-0.20230208065359-62b90e1e24a7+incompatible
	github.com/pingcap/tidb/parser v0.0.0-20230406094457-915b39b8845a
	github.com/prometheus/client_golang v1.14.0
	github.com/prometheus/client_model v0.3.0
	github.com/r3labs/diff v1.1.0
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/segmentio/kafka-go v0.4.39-0.20230217181906-f6986fb02ee7
	github.com/shirou/gopsutil/v3 v3.23.2
>>>>>>> 4ab802a50e (ddl(ticdc): add charset and collate to ddl event (#8723))
	github.com/shopspring/decimal v1.3.0
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cobra v1.2.1
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/swaggo/files v0.0.0-20190704085106-630677cd5c14
	github.com/swaggo/gin-swagger v1.2.0
	github.com/swaggo/swag v1.6.6-0.20200529100950-7c765ddd0476
	github.com/syndtr/goleveldb v1.0.1-0.20210305035536-64b5b1c73954
<<<<<<< HEAD
	github.com/tikv/client-go/v2 v2.0.1-0.20220302130440-46d649305eee
	github.com/tikv/pd v1.1.0-beta.0.20220303060546-3695d8164800
	github.com/tikv/pd/client v0.0.0-20220216080339-1b8f82378ee7
	github.com/tinylib/msgp v1.1.0
=======
	github.com/tikv/client-go/v2 v2.0.7-0.20230328084104-ea13e9700259
	github.com/tikv/pd v1.1.0-beta.0.20230203015356-248b3f0be132
	github.com/tikv/pd/client v0.0.0-20230329114254-1948c247c2b1
	github.com/tinylib/msgp v1.1.6
>>>>>>> 4ab802a50e (ddl(ticdc): add charset and collate to ddl event (#8723))
	github.com/uber-go/atomic v1.4.0
	github.com/vmihailenco/msgpack/v5 v5.3.5
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	go.etcd.io/etcd/api/v3 v3.5.2
	go.etcd.io/etcd/client/pkg/v3 v3.5.2
	go.etcd.io/etcd/client/v3 v3.5.2
	go.etcd.io/etcd/pkg/v3 v3.5.2
	go.etcd.io/etcd/server/v3 v3.5.2
	go.etcd.io/etcd/tests/v3 v3.5.2
<<<<<<< HEAD
	go.uber.org/atomic v1.9.0
	go.uber.org/goleak v1.1.12
	go.uber.org/multierr v1.7.0
	go.uber.org/zap v1.20.0
	golang.org/x/net v0.0.0-20211020060615-d418f374d309
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20211216021012-1d35b9e2eb4e
	golang.org/x/text v0.3.7
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	google.golang.org/genproto v0.0.0-20210825212027-de86158e7fda
	google.golang.org/grpc v1.43.0
=======
	go.uber.org/atomic v1.10.0
	go.uber.org/dig v1.13.0
	go.uber.org/goleak v1.2.1
	go.uber.org/multierr v1.9.0
	go.uber.org/ratelimit v0.2.0
	go.uber.org/zap v1.24.0
	golang.org/x/exp v0.0.0-20221023144134-a1e5550cf13e
	golang.org/x/net v0.8.0
	golang.org/x/sync v0.1.0
	golang.org/x/sys v0.6.0
	golang.org/x/text v0.8.0
	golang.org/x/time v0.3.0
	google.golang.org/genproto v0.0.0-20230202175211-008b39050e57
	google.golang.org/grpc v1.52.3
	google.golang.org/protobuf v1.28.1
>>>>>>> 4ab802a50e (ddl(ticdc): add charset and collate to ddl event (#8723))
	gopkg.in/yaml.v2 v2.4.0
	upper.io/db.v3 v3.7.1+incompatible
)

require (
	cloud.google.com/go v0.93.3 // indirect
	cloud.google.com/go/storage v1.16.1 // indirect
	github.com/99designs/keyring v1.1.5 // indirect
	github.com/AthenZ/athenz v1.10.15 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azcore v0.20.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v0.12.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v0.8.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v0.2.0 // indirect
	github.com/DataDog/zstd v1.4.6-0.20210211175136-c6db21d202f4 // indirect
	github.com/KyleBanks/depth v1.2.1 // indirect
	github.com/PuerkitoBio/purell v1.1.1 // indirect
	github.com/PuerkitoBio/urlesc v0.0.0-20170810143723-de5bf2ad4578 // indirect
	github.com/VividCortex/ewma v1.1.1 // indirect
	github.com/apache/pulsar-client-go/oauth2 v0.0.0-20201120111947-b8bd55bc02bd // indirect
	github.com/apache/thrift v0.13.1-0.20201008052519-daf620915714 // indirect
	github.com/ardielle/ardielle-go v1.5.2 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blacktear23/go-proxyprotocol v0.0.0-20180807104634-af7a81e8dd0d // indirect
	github.com/cakturk/go-netstat v0.0.0-20200220111822-e5b49efee7a5 // indirect
	github.com/carlmjohnson/flagext v0.21.0 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/cheggaaa/pb/v3 v3.0.8 // indirect
	github.com/cockroachdb/errors v1.8.1 // indirect
	github.com/cockroachdb/logtags v0.0.0-20190617123548-eb05cc24525f // indirect
	github.com/cockroachdb/redact v1.0.8 // indirect
	github.com/cockroachdb/sentry-go v0.6.1-cockroachdb.2 // indirect
	github.com/coocood/bbloom v0.0.0-20190830030839-58deb6228d64 // indirect
	github.com/coocood/freecache v1.1.1 // indirect
	github.com/coocood/rtutil v0.0.0-20190304133409-c84515f646f2 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548 // indirect
	github.com/cznic/sortutil v0.0.0-20181122101858-f5f958428db8 // indirect
	github.com/danieljoos/wincred v1.0.2 // indirect
	github.com/danjacques/gofslock v0.0.0-20191023191349-0a45f885bc37 // indirect
	github.com/desertbit/timer v0.0.0-20180107155436-c41aec40b27f // indirect
	github.com/dgraph-io/ristretto v0.0.1 // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/dgryski/go-farm v0.0.0-20190423205320-6a90982ecee2 // indirect
	github.com/dvsekhvalnov/jose2go v0.0.0-20180829124132-7f401d37b68a // indirect
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/form3tech-oss/jwt-go v3.2.5+incompatible // indirect
	github.com/frankban/quicktest v1.11.1 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/gin-contrib/sse v0.1.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-openapi/jsonpointer v0.19.5 // indirect
	github.com/go-openapi/jsonreference v0.19.3 // indirect
	github.com/go-openapi/spec v0.19.4 // indirect
	github.com/go-openapi/swag v0.19.5 // indirect
	github.com/go-playground/locales v0.14.0 // indirect
	github.com/go-playground/universal-translator v0.18.0 // indirect
	github.com/go-playground/validator/v10 v10.9.0 // indirect
	github.com/godbus/dbus v0.0.0-20190726142602-4481cbc300e2 // indirect
	github.com/golang-jwt/jwt v3.2.2+incompatible // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/snappy v0.0.3 // indirect
	github.com/google/pprof v0.0.0-20211122183932-1daafda22083 // indirect
	github.com/googleapis/gax-go/v2 v2.0.5 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/gsterjov/go-libsecret v0.0.0-20161001094733-a6f4afe4910c // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/iancoleman/strcase v0.0.0-20191112232945-16388991a334 // indirect
	github.com/improbable-eng/grpc-web v0.12.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jedib0t/go-pretty/v6 v6.2.2 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/joho/sqltocsv v0.0.0-20210428211105-a6d6801d59df // indirect
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/keybase/go-keychain v0.0.0-20190712205309-48d3d31d256d // indirect
	github.com/klauspost/compress v1.11.7 // indirect
	github.com/klauspost/cpuid v1.2.1 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/labstack/echo/v4 v4.2.1 // indirect
	github.com/labstack/gommon v0.3.0 // indirect
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/mailru/easyjson v0.0.0-20190626092158-b2ccc519800e // indirect
	github.com/mattn/go-colorable v0.1.11 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/mattn/go-runewidth v0.0.12 // indirect
	github.com/mattn/go-sqlite3 v2.0.2+incompatible // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/mtibben/percent v0.2.1 // indirect
	github.com/ncw/directio v1.0.4 // indirect
	github.com/ngaut/log v0.0.0-20180314031856-b8e36e7ba5ac // indirect
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7 // indirect
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef // indirect
	github.com/opentracing/basictracer-go v1.0.0 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pierrec/lz4 v2.5.2+incompatible // indirect
	github.com/pingcap/badger v1.5.1-0.20210831093107-2f6cb8008145 // indirect
	github.com/pingcap/fn v0.0.0-20200306044125-d5540d389059 // indirect
	github.com/pingcap/goleveldb v0.0.0-20191226122134-f82aafb29989 // indirect
	github.com/pingcap/sysutil v0.0.0-20220114020952-ea68d2dbf5b4 // indirect
<<<<<<< HEAD
	github.com/pingcap/tipb v0.0.0-20220110031732-29e23c62eeac // indirect
=======
	github.com/pingcap/tipb v0.0.0-20230310043643-5362260ee6f7 // indirect
>>>>>>> 4ab802a50e (ddl(ticdc): add charset and collate to ddl event (#8723))
	github.com/pkg/browser v0.0.0-20180916011732-0a3d74bf9ce4 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
<<<<<<< HEAD
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/rogpeppe/go-internal v1.8.0 // indirect
=======
	github.com/prometheus/common v0.39.0 // indirect
	github.com/prometheus/procfs v0.9.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20220927061507-ef77025ab5aa // indirect
	github.com/rivo/uniseg v0.4.4 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
>>>>>>> 4ab802a50e (ddl(ticdc): add charset and collate to ddl event (#8723))
	github.com/rs/cors v1.7.0 // indirect
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/shirou/gopsutil/v3 v3.21.12 // indirect
	github.com/shurcooL/httpfs v0.0.0-20190707220628-8d4bc4ba7749 // indirect
	github.com/shurcooL/httpgzip v0.0.0-20190720172056-320755c1c1b0 // indirect
	github.com/shurcooL/vfsgen v0.0.0-20200824052919-0d455de96546 // indirect
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726 // indirect
	github.com/siddontang/go-log v0.0.0-20180807004314-8d05993dda07 // indirect
<<<<<<< HEAD
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/stretchr/objx v0.2.0 // indirect
=======
	github.com/sirupsen/logrus v1.9.0 // indirect
	github.com/spkg/bom v1.0.0 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
>>>>>>> 4ab802a50e (ddl(ticdc): add charset and collate to ddl event (#8723))
	github.com/tiancaiamao/appdash v0.0.0-20181126055449-889f96f722a2 // indirect
	github.com/tklauser/go-sysconf v0.3.9 // indirect
	github.com/tklauser/numcpus v0.3.0 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20201229170055-e5319fda7802 // indirect
	github.com/twmb/murmur3 v1.1.3 // indirect
	github.com/uber/jaeger-client-go v2.22.1+incompatible // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/ugorji/go/codec v1.2.6 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasttemplate v1.2.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/wangjohn/quickselect v0.0.0-20161129230411-ed8402a42d5f // indirect
	github.com/xdg/stringprep v1.0.0 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	github.com/xitongsys/parquet-go v1.6.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.etcd.io/bbolt v1.3.6 // indirect
	go.etcd.io/etcd/client/v2 v2.305.2 // indirect
	go.etcd.io/etcd/raft/v3 v3.5.2 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.opentelemetry.io/contrib v0.20.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.20.0 // indirect
	go.opentelemetry.io/otel v0.20.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp v0.20.0 // indirect
	go.opentelemetry.io/otel/metric v0.20.0 // indirect
	go.opentelemetry.io/otel/sdk v0.20.0 // indirect
	go.opentelemetry.io/otel/sdk/export/metric v0.20.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v0.20.0 // indirect
	go.opentelemetry.io/otel/trace v0.20.0 // indirect
	go.opentelemetry.io/proto/otlp v0.7.0 // indirect
<<<<<<< HEAD
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519 // indirect
	golang.org/x/exp v0.0.0-20200513190911-00229845015e // indirect
	golang.org/x/oauth2 v0.0.0-20210805134026-6f1e6394065a // indirect
	golang.org/x/term v0.0.0-20201126162022-7de9c90e9dd1 // indirect
	golang.org/x/tools v0.1.8 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/api v0.54.0 // indirect
=======
	golang.org/x/crypto v0.7.0 // indirect
	golang.org/x/oauth2 v0.6.0 // indirect
	golang.org/x/term v0.6.0 // indirect
	golang.org/x/tools v0.7.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/api v0.106.0 // indirect
>>>>>>> 4ab802a50e (ddl(ticdc): add charset and collate to ddl event (#8723))
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/jcmturner/aescts.v1 v1.0.1 // indirect
	gopkg.in/jcmturner/dnsutils.v1 v1.0.1 // indirect
	gopkg.in/jcmturner/gokrb5.v7 v7.5.0 // indirect
	gopkg.in/jcmturner/rpc.v1 v1.1.0 // indirect
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
	modernc.org/mathutil v1.4.1 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
	sourcegraph.com/sourcegraph/appdash v0.0.0-20190731080439-ebfcffb1b5c0 // indirect
	sourcegraph.com/sourcegraph/appdash-data v0.0.0-20151005221446-73f23eafcf67 // indirect
)

// Fix CVE-2020-26160.
replace github.com/dgrijalva/jwt-go v3.2.0+incompatible => github.com/golang-jwt/jwt v3.2.1+incompatible
