### Makefile for ticdc
.PHONY: build test check clean fmt cdc kafka_consumer coverage \
	integration_test_build integration_test integration_test_mysql integration_test_kafka bank \
	dm dm-master dm-worker dmctl dm-syncer dm_coverage

PROJECT=tiflow
P=3

FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1  }  }'

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(CURDIR)/bin:$(path_to_add):$(PATH)

SHELL := /usr/bin/env bash

TEST_DIR := /tmp/tidb_cdc_test
DM_TEST_DIR := /tmp/dm_test

GO       := GO111MODULE=on go
ifeq (${CDC_ENABLE_VENDOR}, 1)
GOVENDORFLAG := -mod=vendor
endif

GOBUILD  := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG) -trimpath $(GOVENDORFLAG)
GOBUILDNOVENDOR  := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG) -trimpath
GOTEST   := CGO_ENABLED=1 $(GO) test -p $(P) --race
GOTESTNORACE := CGO_ENABLED=1 $(GO) test -p $(P)

ARCH  := "$(shell uname -s)"
LINUX := "Linux"
MAC   := "Darwin"
CDC_PKG := github.com/pingcap/tiflow
DM_PKG := github.com/pingcap/tiflow/dm
PACKAGE_LIST := go list ./... | grep -vE 'vendor|proto|tiflow\/tests|integration|testing_utils|pb|pbmock|tiflow\/bin'
PACKAGE_LIST_WITHOUT_DM := $(PACKAGE_LIST) | grep -vE 'github.com/pingcap/tiflow/dm'
DM_PACKAGE_LIST := go list github.com/pingcap/tiflow/dm/... | grep -vE 'pb|pbmock|dm/cmd'
PACKAGES := $$($(PACKAGE_LIST))
PACKAGES_WITHOUT_DM := $$($(PACKAGE_LIST_WITHOUT_DM))
DM_PACKAGES := $$($(DM_PACKAGE_LIST))
FILES := $$(find . -name '*.go' -type f | grep -vE 'vendor|kv_gen|proto|pb\.go|pb\.gw\.go')
TEST_FILES := $$(find . -name '*_test.go' -type f | grep -vE 'vendor|kv_gen|integration|testing_utils')
TEST_FILES_WITHOUT_DM := $$(find . -name '*_test.go' -type f | grep -vE 'vendor|kv_gen|integration|testing_utils|^\./dm')
FAILPOINT_DIR := $$(for p in $(PACKAGES); do echo $${p\#"github.com/pingcap/$(PROJECT)/"}|grep -v "github.com/pingcap/$(PROJECT)"; done)
FAILPOINT := tools/bin/failpoint-ctl

FAILPOINT_ENABLE  := $$(echo $(FAILPOINT_DIR) | xargs $(FAILPOINT) enable >/dev/null)
FAILPOINT_DISABLE := $$(echo $(FAILPOINT_DIR) | xargs $(FAILPOINT) disable >/dev/null)

RELEASE_VERSION =
ifeq ($(RELEASE_VERSION),)
	RELEASE_VERSION := v6.1.0-master
	release_version_regex := ^v[0-9]\..*$$
	release_branch_regex := "^release-[0-9]\.[0-9].*$$|^HEAD$$|^.*/*tags/v[0-9]\.[0-9]\..*$$"
	ifneq ($(shell git rev-parse --abbrev-ref HEAD | egrep $(release_branch_regex)),)
		# If we are in release branch, try to use tag version.
		ifneq ($(shell git describe --tags --dirty | egrep $(release_version_regex)),)
			RELEASE_VERSION := $(shell git describe --tags --dirty)
		endif
	else ifneq ($(shell git status --porcelain),)
		# Add -dirty if the working tree is dirty for non release branch.
		RELEASE_VERSION := $(RELEASE_VERSION)-dirty
	endif
endif

BUILDTS := $(shell date -u '+%Y-%m-%d %H:%M:%S')
GITHASH := $(shell git rev-parse HEAD)
GITBRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GOVERSION := $(shell go version)

# CDC LDFLAGS.
LDFLAGS += -X "$(CDC_PKG)/pkg/version.ReleaseVersion=$(RELEASE_VERSION)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.BuildTS=$(BUILDTS)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.GitHash=$(GITHASH)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.GitBranch=$(GITBRANCH)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.GoVersion=$(GOVERSION)"

# DM LDFLAGS.
LDFLAGS += -X "$(DM_PKG)/pkg/utils.ReleaseVersion=$(RELEASE_VERSION)"
LDFLAGS += -X "$(DM_PKG)/pkg/utils.BuildTS=$(BUILDTS)"
LDFLAGS += -X "$(DM_PKG)/pkg/utils.GitHash=$(GITHASH)"
LDFLAGS += -X "$(DM_PKG)/pkg/utils.GitBranch=$(GITBRANCH)"
LDFLAGS += -X "$(DM_PKG)/pkg/utils.GoVersion=$(GOVERSION)"

default: build buildsucc

buildsucc:
	@echo Build TiDB CDC successfully!

all: dev install

dev: check test

test: unit_test dm_unit_test

build: cdc dm

bank:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/bank ./tests/bank/bank.go ./tests/bank/case.go

build-failpoint: check_failpoint_ctl
	$(FAILPOINT_ENABLE)
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc ./cmd/cdc/main.go
	$(FAILPOINT_DISABLE)

cdc:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc ./cmd/cdc/main.go

kafka_consumer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc_kafka_consumer ./cmd/kafka-consumer/main.go

cdc_test_image: 
	@which docker || (echo "docker not found in ${PATH}"; exit 1)
	docker build --platform linux/amd64 -f deployments/ticdc/docker/test.Dockerfile -t cdc:test ./ 

unit_test: check_failpoint_ctl generate_mock generate-msgp-code generate-protobuf
	mkdir -p "$(TEST_DIR)"
	$(FAILPOINT_ENABLE)
	@export log_level=error;\
	$(GOTEST) -cover -covermode=atomic -coverprofile="$(TEST_DIR)/cov.unit.out" $(PACKAGES_WITHOUT_DM) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)

unit_test_in_verify_ci: check_failpoint_ctl tools/bin/gotestsum tools/bin/gocov tools/bin/gocov-xml
	mkdir -p "$(TEST_DIR)"
	$(FAILPOINT_ENABLE)
	@export log_level=error;\
	CGO_ENABLED=1 tools/bin/gotestsum --junitfile cdc-junit-report.xml -- -v -timeout 5m -p $(P) --race \
	-covermode=atomic -coverprofile="$(TEST_DIR)/cov.unit.out" $(PACKAGES_WITHOUT_DM) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	tools/bin/gocov convert "$(TEST_DIR)/cov.unit.out" | tools/bin/gocov-xml > cdc-coverage.xml
	$(FAILPOINT_DISABLE)
	@bash <(curl -s https://codecov.io/bash) -F cdc -f $(TEST_DIR)/cov.unit.out -t $(TICDC_CODECOV_TOKEN)

leak_test: check_failpoint_ctl
	$(FAILPOINT_ENABLE)
	@export log_level=error;\
	$(GOTEST) -count=1 --tags leak $(PACKAGES_WITHOUT_DM) || { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)

check_third_party_binary:
	@which bin/tidb-server
	@which bin/tikv-server
	@which bin/pd-server
	@which bin/tiflash
	@which bin/pd-ctl
	@which bin/sync_diff_inspector
	@which bin/go-ycsb
	@which bin/etcdctl
	@which bin/jq
	@which bin/minio

integration_test_build: check_failpoint_ctl
	$(FAILPOINT_ENABLE)
	$(GOTEST) -ldflags '$(LDFLAGS)' -c -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/tiflow/... \
		-o bin/cdc.test github.com/pingcap/tiflow/cmd/cdc \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc ./cmd/cdc/main.go \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)

integration_test: integration_test_mysql

integration_test_mysql:
	tests/integration_tests/run.sh mysql "$(CASE)" "$(START_AT)"

integration_test_kafka: check_third_party_binary
	tests/integration_tests/run.sh kafka "$(CASE)" "$(START_AT)"

fmt: tools/bin/gofumports tools/bin/shfmt generate_mock generate-msgp-code generate-protobuf
	@echo "gofmt (simplify)"
	tools/bin/gofumports -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)
	@echo "run shfmt"
	tools/bin/shfmt -d -w .
	@echo "check log style"
	scripts/check-log-style.sh

errdoc: tools/bin/errdoc-gen
	@echo "generator errors.toml"
	# check-errdoc will skip DM directory.
	./tools/check/check-errdoc.sh

# terror_check is only used for DM errors.
# TODO: unified the error framework of CDC and DM.
terror_check:
	@echo "check terror conflict"
	@cd dm && _utils/terror_gen/check.sh

check-copyright:
	@echo "check-copyright"
	@./scripts/check-copyright.sh

check-merge-conflicts:
	@echo "check-merge-conflicts"
	@./scripts/check-merge-conflicts.sh

check-leaktest-added: tools/bin/gofumports
	@echo "check leak test added in all unit tests"
	# TODO: enable leaktest for DM tests.
	./scripts/add-leaktest.sh $(TEST_FILES_WITHOUT_DM)

check-ticdc-dashboard:
	@echo "check-ticdc-dashboard"
	@./scripts/check-ticdc-dashboard.sh

check-diff-line-width:
ifneq ($(shell echo $(RELEASE_VERSION) | grep master),)
	@echo "check-file-width"
	@./scripts/check-diff-line-width.sh
endif

generate-msgp-code: tools/bin/msgp
	@echo "generate-msgp-code"
	./scripts/generate-msgp-code.sh

generate-protobuf: tools/bin/protoc tools/bin/protoc-gen-gogofaster
	@echo "generate-protobuf"
	./scripts/generate-protobuf.sh

vet:
	@echo "vet"
	$(GO) vet $(PACKAGES) 2>&1 | $(FAIL_ON_STDOUT)

tidy:
	@echo "go mod tidy"
	./tools/check/check-tidy.sh

# TODO: Unified cdc and dm config.
check-static: tools/bin/golangci-lint
	tools/bin/golangci-lint run --timeout 10m0s --skip-files kv_gen --skip-dirs dm,tests
	cd dm && ../tools/bin/golangci-lint run --timeout 10m0s

check: check-copyright fmt check-static tidy terror_check errdoc check-leaktest-added check-merge-conflicts check-ticdc-dashboard check-diff-line-width swagger-spec
	@git --no-pager diff --exit-code || echo "Please add changed files!"

integration_test_coverage: tools/bin/gocovmerge tools/bin/goveralls
	tools/bin/gocovmerge "$(TEST_DIR)"/cov.* | grep -vE ".*.pb.go|$(CDC_PKG)/testing_utils/.*|$(CDC_PKG)/cdc/entry/schema_test_helper.go|$(CDC_PKG)/cdc/sink/simple_mysql_tester.go|.*.__failpoint_binding__.go" > "$(TEST_DIR)/all_cov.out"
ifeq ("$(JenkinsCI)", "1")
	GO111MODULE=off go get github.com/mattn/goveralls
	tools/bin/goveralls -parallel -coverprofile=$(TEST_DIR)/all_cov.out -service=jenkins-ci -repotoken $(COVERALLS_TOKEN)
else
	go tool cover -html "$(TEST_DIR)/all_cov.out" -o "$(TEST_DIR)/all_cov.html"
endif

unit_test_coverage:
	grep -vE ".*.pb.go|$(CDC_PKG)/testing_utils/.*|$(CDC_PKG)/cdc/sink/simple_mysql_tester.go|.*.__failpoint_binding__.go" "$(TEST_DIR)/cov.unit.out" > "$(TEST_DIR)/unit_cov.out"
	go tool cover -html "$(TEST_DIR)/unit_cov.out" -o "$(TEST_DIR)/unit_cov.html"
	go tool cover -func="$(TEST_DIR)/unit_cov.out"

data-flow-diagram: docs/data-flow.dot
	dot -Tsvg docs/data-flow.dot > docs/data-flow.svg

swagger-spec: tools/bin/swag
	tools/bin/swag init --parseVendor -generalInfo cdc/api/open.go --output docs/swagger

generate_mock: tools/bin/mockgen
	tools/bin/mockgen -source cdc/owner/owner.go -destination cdc/owner/mock/owner_mock.go

clean:
	go clean -i ./...
	rm -rf *.out
	rm -rf bin
	rm -rf tools/bin

dm: dm-master dm-worker dmctl dm-syncer

dm-master:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-master ./dm/cmd/dm-master

dm-master-with-webui:
	@echo "build webui first"
	cd dm/ui && yarn --ignore-scripts && yarn build
	$(GOBUILD) -ldflags '$(LDFLAGS)' -tags dm_webui -o bin/dm-master ./dm/cmd/dm-master

dm-worker:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-worker ./dm/cmd/dm-worker

dmctl:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dmctl ./dm/cmd/dm-ctl

dm-syncer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-syncer ./dm/cmd/dm-syncer

dm-chaos-case:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-chaos-case ./dm/chaos/cases

dm_debug-tools:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/binlog-event-blackhole ./dm/debug-tools/binlog-event-blackhole

dm_generate_proto: tools/bin/protoc-gen-gogofaster tools/bin/protoc-gen-grpc-gateway
	./dm/generate-dm.sh

dm_generate_mock: tools/bin/mockgen
	./dm/tests/generate-mock.sh

dm_generate_openapi: tools/bin/oapi-codegen
	@echo "generate_openapi"
	cd dm && ../tools/bin/oapi-codegen --config=openapi/spec/server-gen-cfg.yaml openapi/spec/dm.yaml
	cd dm && ../tools/bin/oapi-codegen --config=openapi/spec/types-gen-cfg.yaml openapi/spec/dm.yaml
	cd dm && ../tools/bin/oapi-codegen --config=openapi/spec/client-gen-cfg.yaml openapi/spec/dm.yaml

define run_dm_unit_test
	@echo "running unit test for packages:" $(1)
	mkdir -p $(DM_TEST_DIR)
	$(FAILPOINT_ENABLE)
	@export log_level=error; \
	$(GOTEST) -timeout 10m -covermode=atomic -coverprofile="$(DM_TEST_DIR)/cov.unit_test.out" $(1) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)
endef

dm_unit_test: check_failpoint_ctl
	$(call run_dm_unit_test,$(DM_PACKAGES))

# run unit test for the specified pkg only, like `make dm_unit_test_pkg PKG=github.com/pingcap/tiflow/dm/dm/master`
dm_unit_test_pkg: check_failpoint_ctl
	$(call run_dm_unit_test,$(PKG))

dm_unit_test_in_verify_ci: check_failpoint_ctl tools/bin/gotestsum tools/bin/gocov tools/bin/gocov-xml
	mkdir -p $(DM_TEST_DIR)
	$(FAILPOINT_ENABLE)
	@export log_level=error; \
	CGO_ENABLED=1 tools/bin/gotestsum --junitfile dm-junit-report.xml -- -v -timeout 10m -p $(P) --race \
	-covermode=atomic -coverprofile="$(DM_TEST_DIR)/cov.unit_test.out" $(DM_PACKAGES) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	tools/bin/gocov convert "$(DM_TEST_DIR)/cov.unit_test.out" | tools/bin/gocov-xml > dm-coverage.xml
	$(FAILPOINT_DISABLE)
	@bash <(curl -s https://codecov.io/bash) -F dm -f $(DM_TEST_DIR)/cov.unit_test.out -t $(TICDC_CODECOV_TOKEN)

dm_integration_test_build: check_failpoint_ctl
	$(FAILPOINT_ENABLE)
	$(GOTEST) -ldflags '$(LDFLAGS)' -c -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/tiflow/dm/... \
		-o bin/dm-worker.test github.com/pingcap/tiflow/dm/cmd/dm-worker \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(GOTEST) -ldflags '$(LDFLAGS)' -c -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/tiflow/dm/... \
		-o bin/dm-master.test github.com/pingcap/tiflow/dm/cmd/dm-master \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(GOTESTNORACE) -ldflags '$(LDFLAGS)' -c -cover -covermode=count \
		-coverpkg=github.com/pingcap/tiflow/dm/... \
		-o bin/dmctl.test github.com/pingcap/tiflow/dm/cmd/dm-ctl \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(GOTEST) -ldflags '$(LDFLAGS)' -c -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/tiflow/dm/... \
		-o bin/dm-syncer.test github.com/pingcap/tiflow/dm/cmd/dm-syncer \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)
	./dm/tests/prepare_tools.sh

dm_integration_test_build_worker: check_failpoint_ctl
	$(FAILPOINT_ENABLE)
	$(GOTEST) -ldflags '$(LDFLAGS)' -c -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/tiflow/dm/... \
		-o bin/dm-worker.test github.com/pingcap/tiflow/dm/cmd/dm-worker \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)
	./dm/tests/prepare_tools.sh

dm_integration_test_build_master: check_failpoint_ctl
	$(FAILPOINT_ENABLE)
	$(GOTEST) -ldflags '$(LDFLAGS)' -c -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/tiflow/dm/... \
		-o bin/dm-master.test github.com/pingcap/tiflow/dm/cmd/dm-master \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)
	./dm/tests/prepare_tools.sh

dm_integration_test_build_ctl: check_failpoint_ctl
	$(FAILPOINT_ENABLE)
	$(GOTESTNORACE) -ldflags '$(LDFLAGS)' -c -cover -covermode=count \
		-coverpkg=github.com/pingcap/tiflow/dm/... \
		-o bin/dmctl.test github.com/pingcap/tiflow/dm/cmd/dm-ctl \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)
	./dm/tests/prepare_tools.sh

install_test_python_dep:
	@echo "install python requirments for test"
	pip install --user -q -r ./dm/tests/requirements.txt

check_third_party_binary_for_dm:
	@which bin/tidb-server
	@which bin/sync_diff_inspector
	@which mysql
	@which bin/minio

dm_integration_test: check_third_party_binary_for_dm install_test_python_dep
	@which bin/dm-master.test
	@which bin/dm-worker.test
	@which bin/dm-syncer.test
	cd dm && ln -sf ../bin .
	cd dm && ./tests/run.sh $(CASE)

dm_compatibility_test: check_third_party_binary_for_dm
	@which bin/dm-master.test.current
	@which bin/dm-worker.test.current
	@which bin/dm-master.test.previous
	@which bin/dm-worker.test.previous
	cd dm && ln -sf ../bin .
	cd dm && ./tests/compatibility_run.sh ${CASE}

dm_coverage: tools/bin/gocovmerge tools/bin/goveralls
	# unify cover mode in coverage files, more details refer to dm/tests/_utils/run_dm_ctl
	find "$(DM_TEST_DIR)" -type f -name "cov.*.dmctl.*.out" -exec sed -i "s/mode: count/mode: atomic/g" {} \;
	tools/bin/gocovmerge "$(DM_TEST_DIR)"/cov.* | grep -vE ".*.pb.go|.*.pb.gw.go|.*.__failpoint_binding__.go|.*debug-tools.*|.*chaos.*" > "$(DM_TEST_DIR)/all_cov.out"
	tools/bin/gocovmerge "$(DM_TEST_DIR)"/cov.unit_test*.out | grep -vE ".*.pb.go|.*.pb.gw.go|.*.__failpoint_binding__.go|.*debug-tools.*|.*chaos.*" > $(DM_TEST_DIR)/unit_test.out
	go tool cover -html "$(DM_TEST_DIR)/all_cov.out" -o "$(DM_TEST_DIR)/all_cov.html"
	go tool cover -html "$(DM_TEST_DIR)/unit_test.out" -o "$(DM_TEST_DIR)/unit_test_cov.html"

tools/bin/failpoint-ctl: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/failpoint-ctl github.com/pingcap/failpoint/failpoint-ctl

tools/bin/gocovmerge: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gocovmerge github.com/zhouqiang-cl/gocovmerge

tools/bin/goveralls: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/goveralls github.com/mattn/goveralls

tools/bin/golangci-lint: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/golangci-lint github.com/golangci/golangci-lint/cmd/golangci-lint

tools/bin/mockgen: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/mockgen github.com/golang/mock/mockgen

tools/bin/protoc-gen-gogofaster: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/protoc-gen-gogofaster github.com/gogo/protobuf/protoc-gen-gogofaster

tools/bin/protoc-gen-grpc-gateway: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/protoc-gen-grpc-gateway github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway

tools/bin/gofumports: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gofumports mvdan.cc/gofumpt

tools/bin/shfmt: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/shfmt mvdan.cc/sh/v3/cmd/shfmt

tools/bin/oapi-codegen: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/oapi-codegen github.com/deepmap/oapi-codegen/cmd/oapi-codegen

tools/bin/gocov: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gocov  github.com/axw/gocov/gocov

tools/bin/gocov-xml: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gocov-xml github.com/AlekSi/gocov-xml

tools/bin/gotestsum: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/gotestsum gotest.tools/gotestsum

tools/bin/errdoc-gen: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/errdoc-gen github.com/pingcap/errors/errdoc-gen

tools/bin/swag: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/swag github.com/swaggo/swag/cmd/swag

tools/bin/msgp: tools/check/go.mod
	cd tools/check && $(GO) build -mod=mod -o ../bin/msgp github.com/tinylib/msgp

tools/bin/protoc:
	./scripts/download-protoc.sh

check_failpoint_ctl: tools/bin/failpoint-ctl

failpoint-enable: check_failpoint_ctl
	$(FAILPOINT_ENABLE)

failpoint-disable: check_failpoint_ctl
	$(FAILPOINT_DISABLE)
