GO := CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go

.PHONY: run fmt

build:
	$(GO) build -o bin/workload main.go

image:
	GOOS=linux GOARCH=amd64 make build
	docker build -t hub.pingcap.net/qa/cdc-boundary-workload .

fmt: groupimports
	go fmt ./...

groupimports: install-goimports
	goimports -w -l -local github.com/pingcap/test-infra .

install-goimports:
ifeq (, $(shell which goimports))
	@{ \
	set -e ;\
	TMP_DIR=$$(mktemp -d) ;\
	cd $$TMP_DIR ;\
	GO111MODULE=on go get golang.org/x/tools/cmd/goimports ;\
	rm -rf $$TMP_DIR ;\
	}
endif


