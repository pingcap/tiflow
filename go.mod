module github.com/pingcap/ticdc

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/DATA-DOG/go-sqlmock v1.3.3 // indirect
	github.com/Shopify/sarama v1.26.1
	github.com/biogo/store v0.0.0-20190426020002-884f370e325d
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/chzyer/readline v0.0.0-20171208011716-f6d7a1f6fbf3
	github.com/edwingeng/deque v0.0.0-20191220032131-8596380dee17
	github.com/go-sql-driver/mysql v1.4.1
	github.com/golang/groupcache v0.0.0-20191227052852-215e87163ea7 // indirect
	github.com/google/btree v1.0.0
	github.com/google/uuid v1.1.1
	github.com/gorilla/websocket v1.4.1 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/klauspost/compress v1.10.2 // indirect
	github.com/mattn/go-shellwords v1.0.3
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/kvproto v0.0.0-20200330093347-98f910b71904
	github.com/pingcap/log v0.0.0-20200117041106-d28c14d3b1cd
	github.com/pingcap/parser v0.0.0-20200317021010-cd90cc2a7d87
	github.com/pingcap/pd/v4 v4.0.0-beta.1.0.20200305072537-61d9f9cc35d3
	github.com/pingcap/tidb v1.1.0-beta.0.20200212043647-e66daf3e04d0
	github.com/pingcap/tidb-tools v4.0.0-beta.1.0.20200306084441-875bd09aa3d5+incompatible
	github.com/prometheus/client_golang v1.3.0
	github.com/prometheus/common v0.8.0 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/testify v1.4.0
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191211224106-0dc78a144b31
	go.uber.org/zap v1.14.0
	golang.org/x/crypto v0.0.0-20200302210943-78000ba7a073 // indirect
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0
	google.golang.org/appengine v1.6.2 // indirect
	google.golang.org/genproto v0.0.0-20200113173426-e1de0a7b01eb // indirect
	google.golang.org/grpc v1.26.0
)

replace github.com/pingcap/tidb => github.com/MyonKeminta/tidb v1.1.0-alpha.1.0.20200325092028-b9f4e8c44d23
