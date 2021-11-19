all: df-proto df-master df-executor df-master-client producer

df-proto:
	./generate-proto.sh

df-master:
	go build -o bin/master ./cmd/master

df-executor:
	go build -o bin/executor ./cmd/executor

df-master-client:
	go build -o bin/master-client ./cmd/master-client

producer:
	go build -o bin/producer ./cmd/producer
