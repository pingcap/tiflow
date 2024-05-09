### Makefile for tiflow
.PHONY: build test check clean fmt cdc kafka_consumer storage_consumer coverage \
	integration_test_build integration_test integration_test_mysql integration_test_kafka bank \
	kafka_docker_integration_test kafka_docker_integration_test_with_build \
	clean_integration_test_containers \
	mysql_docker_integration_test mysql_docker_integration_test_with_build \
	build_mysql_integration_test_images clean_integration_test_images \
	dm dm-master dm-worker dmctl dm-syncer dm_coverage \
	engine tiflow tiflow-demo tiflow-chaos-case engine_image help \
	format-makefiles check-makefiles oauth2_server prepare_test_binaries


.DEFAULT_GOAL := default

# Adapted from https://www.thapaliya.com/en/writings/well-documented-makefiles/
help: ## Display this help and any documented user-facing targets. Other undocumented targets may be present in the Makefile.
help:
	@awk 'BEGIN {FS = ": ##"; printf "Usage:\n  make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_\.\-\/%]+: ##/ { printf "  %-45s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

PROJECT=tiflow
P=3

FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1  }  }'

CURDIR := $(shell pwd)
path_to_add := $(addsuffix /bin,$(subst :,/bin:,$(GOPATH)))
export PATH := $(CURDIR)/bin:$(CURDIR)/tools/bin:$(path_to_add):$(PATH)

# DBUS_SESSION_BUS_ADDRESS pulsar client use dbus to detect the connection status,
# but it will not exit when the connection is closed.
# I try to use leak_helper to detect goroutine leak,but it does not work.
# https://github.com/benthosdev/benthos/issues/1184 suggest to use environment variable to disable dbus.
export DBUS_SESSION_BUS_ADDRESS := /dev/null

SHELL := /usr/bin/env bash

TEST_DIR := /tmp/tidb_cdc_test
DM_TEST_DIR := /tmp/dm_test
ENGINE_TEST_DIR := /tmp/engine_test

GO       := GO111MODULE=on go
ifeq (${CDC_ENABLE_VENDOR}, 1)
GOVENDORFLAG := -mod=vendor
endif

# Since TiDB add a new dependency on github.com/cloudfoundry/gosigar,
# We need to add CGO_ENABLED=1 to make it work when build TiCDC in Darwin OS.
# These logic is to check if the OS is Darwin, if so, add CGO_ENABLED=1.
# ref: https://github.com/cloudfoundry/gosigar/issues/58#issuecomment-1150925711
# ref: https://github.com/pingcap/tidb/pull/39526#issuecomment-1407952955
OS := "$(shell go env GOOS)"
SED_IN_PLACE ?= $(shell which sed)
IS_ALPINE := $(shell grep -qi Alpine /etc/os-release && echo 1)
ifeq (${OS}, "linux")
	CGO := 0
	SED_IN_PLACE += -i
else ifeq (${OS}, "darwin")
	CGO := 1
	SED_IN_PLACE += -i ''
endif

BUILD_FLAG =
GOEXPERIMENT=
ifeq ("${ENABLE_FIPS}", "1")
	BUILD_FLAG = -tags boringcrypto
	GOEXPERIMENT = GOEXPERIMENT=boringcrypto
	CGO = 1
endif

CONSUMER_BUILD_FLAG=
ifeq ("${IS_ALPINE}", "1")
	CONSUMER_BUILD_FLAG = -tags musl
endif
GOBUILD  := $(GOEXPERIMENT) CGO_ENABLED=$(CGO) $(GO) build $(BUILD_FLAG) -trimpath $(GOVENDORFLAG)
CONSUMER_GOBUILD  := $(GOEXPERIMENT) CGO_ENABLED=1 $(GO) build $(CONSUMER_BUILD_FLAG) -trimpath $(GOVENDORFLAG)
GOBUILDNOVENDOR  := CGO_ENABLED=0 $(GO) build $(BUILD_FLAG) -trimpath
GOTEST   := CGO_ENABLED=1 $(GO) test -p $(P) --race --tags=intest
GOTESTNORACE := CGO_ENABLED=1 $(GO) test -p $(P)

CDC_PKG := github.com/pingcap/tiflow
DM_PKG := github.com/pingcap/tiflow/dm
ENGINE_PKG := github.com/pingcap/tiflow/engine
PACKAGE_LIST := go list ./... | grep -vE 'vendor|proto|tiflow/tests|integration|testing_utils|pb|pbmock|tiflow/bin'
PACKAGE_LIST_WITHOUT_DM_ENGINE := $(PACKAGE_LIST) | grep -vE 'github.com/pingcap/tiflow/cmd|github.com/pingcap/tiflow/dm|github.com/pingcap/tiflow/engine'
DM_PACKAGE_LIST := go list github.com/pingcap/tiflow/dm/... | grep -vE 'pb|pbmock'
PACKAGES := $$($(PACKAGE_LIST))
PACKAGES_TICDC := $$($(PACKAGE_LIST_WITHOUT_DM_ENGINE))
DM_PACKAGES := $$($(DM_PACKAGE_LIST))
# NOTE: ignore engine/framework because of a race in testify. See #9619
# ENGINE_PACKAGE_LIST := go list github.com/pingcap/tiflow/engine/... | grep -vE 'pb|proto|engine/test/e2e'
ENGINE_PACKAGE_LIST := go list github.com/pingcap/tiflow/engine/... | grep -vE "pb|proto|engine/test/e2e|engine/framework$$"
ENGINE_PACKAGES := $$($(ENGINE_PACKAGE_LIST))
FILES := $$(find . -name '*.go' -type f | grep -vE 'vendor|_gen|proto|pb\.go|pb\.gw\.go|_mock.go')
TEST_FILES := $$(find . -name '*_test.go' -type f | grep -vE 'vendor|kv_gen|integration|testing_utils')
TEST_FILES_WITHOUT_DM := $$(find . -name '*_test.go' -type f | grep -vE 'vendor|kv_gen|integration|testing_utils|^\./dm')
FAILPOINT_DIR := $$(for p in $(PACKAGES); do echo $${p\#"github.com/pingcap/$(PROJECT)/"}|grep -v "github.com/pingcap/$(PROJECT)"; done)
FAILPOINT := tools/bin/failpoint-ctl

FAILPOINT_ENABLE  := $$(echo $(FAILPOINT_DIR) | xargs $(FAILPOINT) enable >/dev/null)
FAILPOINT_DISABLE := $$(echo $(FAILPOINT_DIR) | xargs $(FAILPOINT) disable >/dev/null)

TICDC_DOCKER_DEPLOYMENTS_DIR := deployments/ticdc/docker-compose/

# MAKE_FILES is a list of make files to lint.
# We purposefully avoid MAKEFILES as a variable name as it influences
# the files included in recursive invocations of make
MAKE_FILES = $(shell find . \( -name 'Makefile' -o -name '*.mk' \) -print)

RELEASE_VERSION =
ifeq ($(RELEASE_VERSION),)
	RELEASE_VERSION := v8.2.0-master
	release_version_regex := ^v[0-9]\..*$$
	release_branch_regex := "^release-[0-9]\.[0-9].*$$|^HEAD$$|^.*/*tags/v[0-9]\.[0-9]\..*$$"
	ifneq ($(shell git rev-parse --abbrev-ref HEAD | grep -E $(release_branch_regex)),)
		# If we are in release branch, try to use tag version.
		ifneq ($(shell git describe --tags --dirty | grep -E $(release_version_regex)),)
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

# Version LDFLAGS.
LDFLAGS += -X "$(CDC_PKG)/pkg/version.ReleaseVersion=$(RELEASE_VERSION)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.BuildTS=$(BUILDTS)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.GitHash=$(GITHASH)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.GitBranch=$(GITBRANCH)"
LDFLAGS += -X "$(CDC_PKG)/pkg/version.GoVersion=$(GOVERSION)"
LDFLAGS += -X "github.com/pingcap/tidb/pkg/parser/mysql.TiDBReleaseVersion=$(RELEASE_VERSION)"

include tools/Makefile
include Makefile.engine

default: build buildsucc

buildsucc:
	@echo Build TiDB CDC successfully!

all: dev install

dev: check test

test: unit_test dm_unit_test engine_unit_test

build: cdc dm engine

check-makefiles: ## Check the makefiles format. Please run this target after the changes are committed.
check-makefiles: format-makefiles
	@git diff --exit-code -- $(MAKE_FILES) || (echo "Please format Makefiles by running 'make format-makefiles'" && false)

format-makefiles: ## Format all Makefiles.
format-makefiles: $(MAKE_FILES)
	$(SED_IN_PLACE) -e 's/^\(\t*\)  /\1\t/g' -e 's/^\(\t*\) /\1/' -- $?

bank:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/bank ./tests/bank/bank.go ./tests/bank/case.go

build-cdc-with-failpoint: check_failpoint_ctl
build-cdc-with-failpoint: ## Build cdc with failpoint enabled.
	$(FAILPOINT_ENABLE)
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc ./cmd/cdc/main.go
	$(FAILPOINT_DISABLE)

cdc:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc ./cmd/cdc

kafka_consumer:
	$(CONSUMER_GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc_kafka_consumer ./cmd/kafka-consumer

storage_consumer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc_storage_consumer ./cmd/storage-consumer/main.go

pulsar_consumer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc_pulsar_consumer ./cmd/pulsar-consumer/main.go

oauth2_server:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/oauth2-server ./cmd/oauth2-server/main.go

kafka_consumer_image:
	@which docker || (echo "docker not found in ${PATH}"; exit 1)
	DOCKER_BUILDKIT=1 docker build -f ./deployments/ticdc/docker/kafka-consumer.Dockerfile . -t ticdc:kafka-consumer  --platform linux/amd64

storage_consumer_image:
	@which docker || (echo "docker not found in ${PATH}"; exit 1)
	DOCKER_BUILDKIT=1 docker build -f ./deployments/ticdc/docker/storage-consumer.Dockerfile . -t ticdc:storage-consumer  --platform linux/amd64

pulsar_consumer_image:
	@which docker || (echo "docker not found in ${PATH}"; exit 1)
	DOCKER_BUILDKIT=1 docker build -f ./deployments/ticdc/docker/pulsar-consumer.Dockerfile . -t ticdc:pulsar-consumer  --platform linux/amd64

filter_helper:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/cdc_filter_helper ./cmd/filter-helper/main.go

install:
	go install ./...

unit_test: check_failpoint_ctl generate_mock go-generate generate-protobuf
	mkdir -p "$(TEST_DIR)"
	$(FAILPOINT_ENABLE)
	@export log_level=error;\
	$(GOTEST) -cover -covermode=atomic -coverprofile="$(TEST_DIR)/cov.unit.out" $(PACKAGES_TICDC) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)

unit_test_in_verify_ci: check_failpoint_ctl tools/bin/gotestsum tools/bin/gocov tools/bin/gocov-xml
	mkdir -p "$(TEST_DIR)"
	$(FAILPOINT_ENABLE)
	@export log_level=error;\
	CGO_ENABLED=1 tools/bin/gotestsum --junitfile cdc-junit-report.xml -- -v -timeout 5m -p $(P) --race --tags=intest \
	-covermode=atomic -coverprofile="$(TEST_DIR)/cov.unit.out" $(PACKAGES_TICDC) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	tools/bin/gocov convert "$(TEST_DIR)/cov.unit.out" | tools/bin/gocov-xml > cdc-coverage.xml
	$(FAILPOINT_DISABLE)

leak_test: check_failpoint_ctl
	$(FAILPOINT_ENABLE)
	@export log_level=error;\
	$(GOTEST) -count=1 --tags leak $(PACKAGES_TICDC) || { $(FAILPOINT_DISABLE); exit 1; }
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
	@which bin/bin/schema-registry-start

integration_test_build: check_failpoint_ctl storage_consumer kafka_consumer pulsar_consumer oauth2_server
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

mysql_docker_integration_test: ## Run TiCDC MySQL all integration tests in Docker.
mysql_docker_integration_test: clean_integration_test_containers
	docker-compose -f $(TICDC_DOCKER_DEPLOYMENTS_DIR)/docker-compose-mysql-integration.yml up

mysql_docker_integration_test_with_build: ## Build images and run TiCDC MySQL all integration tests in Docker. Please use only after modifying the TiCDC non-test code.
mysql_docker_integration_test_with_build: clean_integration_test_containers
	docker-compose -f $(TICDC_DOCKER_DEPLOYMENTS_DIR)/docker-compose-mysql-integration.yml up --build

build_mysql_integration_test_images: ## Build MySQL integration test images without cache.
build_mysql_integration_test_images: clean_integration_test_containers
	docker-compose -f $(TICDC_DOCKER_DEPLOYMENTS_DIR)/docker-compose-mysql-integration.yml build --no-cache

integration_test_kafka: check_third_party_binary
	tests/integration_tests/run.sh kafka "$(CASE)" "$(START_AT)"

integration_test_storage:
	tests/integration_tests/run.sh storage "$(CASE)" "$(START_AT)"

integration_test_pulsar:
	tests/integration_tests/run.sh pulsar "$(CASE)" "$(START_AT)"

kafka_docker_integration_test: ## Run TiCDC Kafka all integration tests in Docker.
kafka_docker_integration_test: clean_integration_test_containers
	docker-compose -f $(TICDC_DOCKER_DEPLOYMENTS_DIR)/docker-compose-kafka-integration.yml up

kafka_docker_integration_test_with_build: ## Build images and run TiCDC Kafka all integration tests in Docker. Please use only after modifying the TiCDC non-test code.
kafka_docker_integration_test_with_build: clean_integration_test_containers
	docker-compose -f $(TICDC_DOCKER_DEPLOYMENTS_DIR)/docker-compose-kafka-integration.yml up --build

build_kafka_integration_test_images: ## Build Kafka integration test images without cache.
build_kafka_integration_test_images: clean_integration_test_containers
	docker-compose -f $(TICDC_DOCKER_DEPLOYMENTS_DIR)/docker-compose-kafka-integration.yml build --no-cache

clean_integration_test_containers: ## Clean MySQL and Kafka integration test containers.
	docker-compose -f $(TICDC_DOCKER_DEPLOYMENTS_DIR)/docker-compose-mysql-integration.yml down -v
	docker-compose -f $(TICDC_DOCKER_DEPLOYMENTS_DIR)/docker-compose-kafka-integration.yml down -v

fmt: tools/bin/gofumports tools/bin/shfmt tools/bin/gci
	@echo "run gci (format imports)"
	tools/bin/gci write $(FILES) 2>&1 | $(FAIL_ON_STDOUT)
	@echo "run gofumports"
	tools/bin/gofumports -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)
	@echo "run shfmt"
	tools/bin/shfmt -d -w .
	@echo "check log style"
	scripts/check-log-style.sh
	@make check-diff-line-width

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

check-ticdc-dashboard:
	@echo "check-ticdc-dashboard"
	@./scripts/check-ticdc-dashboard.sh

check-diff-line-width:
ifneq ($(shell echo $(RELEASE_VERSION) | grep master),)
	@echo "check-file-width"
	@./scripts/check-diff-line-width.sh
endif

go-generate: ## Run go generate on all packages.
go-generate: tools/bin/msgp tools/bin/stringer tools/bin/mockery
	@echo "go generate"
	@go generate ./...

generate-protobuf: ## Generate code from protobuf files.
generate-protobuf: tools/bin/protoc tools/bin/protoc-gen-gogofaster \
	tools/bin/protoc-gen-go tools/bin/protoc-gen-go-grpc \
	tools/bin/protoc-gen-grpc-gateway tools/bin/protoc-gen-grpc-gateway-v2 \
	tools/bin/protoc-gen-openapiv2
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
	tools/bin/golangci-lint run --timeout 10m0s --skip-dirs "^dm/","^tests/"
	cd dm && ../tools/bin/golangci-lint run --timeout 10m0s

check: check-copyright generate_mock go-generate fmt check-static tidy terror_check errdoc \
	check-merge-conflicts check-ticdc-dashboard check-diff-line-width check-makefiles \
	check_cdc_integration_test check_dm_integration_test check_engine_integration_test
	@git --no-pager diff --exit-code || (echo "Please add changed files!" && false)

fast_check: check-copyright fmt check-static tidy terror_check errdoc \
	check-merge-conflicts check-ticdc-dashboard check-diff-line-width swagger-spec check-makefiles \
	check_cdc_integration_test check_dm_integration_test check_engine_integration_test
	@git --no-pager diff --exit-code || (echo "Please add changed files!" && false)

integration_test_coverage: tools/bin/gocovmerge tools/bin/goveralls
	tools/bin/gocovmerge "$(TEST_DIR)"/cov.* | grep -vE ".*.pb.go|$(CDC_PKG)/testing_utils/.*|$(CDC_PKG)/cdc/entry/schema_test_helper.go|$(CDC_PKG)/cdc/sink/simple_mysql_tester.go|.*.__failpoint_binding__.go" > "$(TEST_DIR)/all_cov.out"
ifeq ("$(JenkinsCI)", "1")
	GO111MODULE=off go get github.com/mattn/goveralls
	tools/bin/goveralls -parallel -coverprofile=$(TEST_DIR)/all_cov.out -service=jenkins-ci -repotoken $(COVERALLS_TOKEN)
else
	go tool cover -html "$(TEST_DIR)/all_cov.out" -o "$(TEST_DIR)/all_cov.html"
endif

swagger-spec: tools/bin/swag
	tools/bin/swag init --exclude dm,engine --parseVendor -generalInfo cdc/api/v1/api.go --output docs/swagger

unit_test_coverage:
	grep -vE ".*.pb.go|$(CDC_PKG)/testing_utils/.*|$(CDC_PKG)/cdc/sink/simple_mysql_tester.go|.*.__failpoint_binding__.go" "$(TEST_DIR)/cov.unit.out" > "$(TEST_DIR)/unit_cov.out"
	go tool cover -html "$(TEST_DIR)/unit_cov.out" -o "$(TEST_DIR)/unit_cov.html"
	go tool cover -func="$(TEST_DIR)/unit_cov.out"

data-flow-diagram: docs/data-flow.dot
	dot -Tsvg docs/data-flow.dot > docs/data-flow.svg

generate_mock: ## Generate mock code.
generate_mock: tools/bin/mockgen
	scripts/generate-mock.sh

clean:
	go clean -i ./...
	rm -rf *.out
	rm -rf bin
	rm -rf tools/bin
	rm -rf tools/include

dm: dm-master dm-worker dmctl dm-syncer

dm-master:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-master ./cmd/dm-master

dm-master-with-webui:
	@echo "build webui first"
	cd dm/ui && yarn --ignore-scripts && yarn build
	$(GOBUILD) -ldflags '$(LDFLAGS)' -tags dm_webui -o bin/dm-master ./cmd/dm-master

dm-worker:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-worker ./cmd/dm-worker

dmctl:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dmctl ./cmd/dm-ctl

dm-syncer:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-syncer ./cmd/dm-syncer

dm-chaos-case:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/dm-chaos-case ./dm/chaos/cases

dm_debug-tools:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/binlog-event-blackhole ./dm/debug-tools/binlog-event-blackhole

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

# run unit test for the specified pkg only, like `make dm_unit_test_pkg PKG=github.com/pingcap/tiflow/dm/master`
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

dm_integration_test_build: check_failpoint_ctl
	$(FAILPOINT_ENABLE)
	$(GOTEST) -ldflags '$(LDFLAGS)' -c -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/tiflow/dm/... \
		-o bin/dm-worker.test github.com/pingcap/tiflow/cmd/dm-worker \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(GOTEST) -ldflags '$(LDFLAGS)' -c -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/tiflow/dm/... \
		-o bin/dm-master.test github.com/pingcap/tiflow/cmd/dm-master \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(GOTESTNORACE) -ldflags '$(LDFLAGS)' -c -cover -covermode=count \
		-coverpkg=github.com/pingcap/tiflow/dm/... \
		-o bin/dmctl.test github.com/pingcap/tiflow/cmd/dm-ctl \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(GOTEST) -ldflags '$(LDFLAGS)' -c -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/tiflow/dm/... \
		-o bin/dm-syncer.test github.com/pingcap/tiflow/cmd/dm-syncer \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)
	./dm/tests/prepare_tools.sh

dm_integration_test_build_worker: check_failpoint_ctl
	$(FAILPOINT_ENABLE)
	$(GOTEST) -ldflags '$(LDFLAGS)' -c -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/tiflow/dm/... \
		-o bin/dm-worker.test github.com/pingcap/tiflow/cmd/dm-worker \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)
	./dm/tests/prepare_tools.sh

dm_integration_test_build_master: check_failpoint_ctl
	$(FAILPOINT_ENABLE)
	$(GOTEST) -ldflags '$(LDFLAGS)' -c -cover -covermode=atomic \
		-coverpkg=github.com/pingcap/tiflow/dm/... \
		-o bin/dm-master.test github.com/pingcap/tiflow/cmd/dm-master \
		|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)
	./dm/tests/prepare_tools.sh

dm_integration_test_build_ctl: check_failpoint_ctl
	$(FAILPOINT_ENABLE)
	$(GOTESTNORACE) -ldflags '$(LDFLAGS)' -c -cover -covermode=count \
		-coverpkg=github.com/pingcap/tiflow/dm/... \
		-o bin/dmctl.test github.com/pingcap/tiflow/cmd/dm-ctl \
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

dm_integration_test_in_group: check_third_party_binary_for_dm install_test_python_dep
	@which bin/dm-master.test
	@which bin/dm-worker.test
	@which bin/dm-syncer.test
	cd dm && ln -sf ../bin .
	cd dm && ./tests/run_group.sh $(GROUP)

dm_compatibility_test: check_third_party_binary_for_dm
	@which bin/dm-master.test.current
	@which bin/dm-worker.test.current
	@which bin/dm-master.test.previous
	@which bin/dm-worker.test.previous
	cd dm && ln -sf ../bin .
	cd dm && ./tests/compatibility_run.sh ${CASE}

dm_coverage: tools/bin/gocovmerge tools/bin/goveralls
	# unify cover mode in coverage files, more details refer to dm/tests/_utils/run_dm_ctl
	find "$(DM_TEST_DIR)" -type f -name "cov.*.dmctl.*.out" -exec $(SED_IN_PLACE) "s/mode: count/mode: atomic/g" {} \;
	tools/bin/gocovmerge "$(DM_TEST_DIR)"/cov.* | grep -vE ".*.pb.go|.*.pb.gw.go|.*.__failpoint_binding__.go|.*debug-tools.*|.*chaos.*" > "$(DM_TEST_DIR)/all_cov.out"
	tools/bin/gocovmerge "$(DM_TEST_DIR)"/cov.unit_test*.out | grep -vE ".*.pb.go|.*.pb.gw.go|.*.__failpoint_binding__.go|.*debug-tools.*|.*chaos.*" > $(DM_TEST_DIR)/unit_test.out
	go tool cover -html "$(DM_TEST_DIR)/all_cov.out" -o "$(DM_TEST_DIR)/all_cov.html"
	go tool cover -html "$(DM_TEST_DIR)/unit_test.out" -o "$(DM_TEST_DIR)/unit_test_cov.html"


check_failpoint_ctl: tools/bin/failpoint-ctl

failpoint-enable: check_failpoint_ctl
	$(FAILPOINT_ENABLE)

failpoint-disable: check_failpoint_ctl
	$(FAILPOINT_DISABLE)

engine: tiflow

tiflow:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/tiflow ./cmd/tiflow/main.go

tiflow-demo:
	@echo "this demo is deprecated, will be removed in next version"

tiflow-chaos-case:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/tiflow-chaos-case ./engine/chaos/cases

engine_unit_test: check_failpoint_ctl
	$(call run_engine_unit_test,$(ENGINE_PACKAGES))

engine_integration_test: check_third_party_binary_for_engine
	mkdir -p /tmp/tiflow_engine_test || true
	./engine/test/integration_tests/run.sh "$(CASE)" "$(START_AT)" 2>&1 | tee /tmp/tiflow_engine_test/engine_it.log
	./engine/test/utils/check_log.sh

check_third_party_binary_for_engine:
	@which bash || (echo "bash not found in ${PATH}"; exit 1)
	@which docker || (echo "docker not found in ${PATH}"; exit 1)
	@which go || (echo "go not found in ${PATH}"; exit 1)
	@which mysql || (echo "mysql not found in ${PATH}"; exit 1)
	@which jq || (echo "jq not found in ${PATH}"; exit 1)
	@which mc || (echo "mc not found in ${PATH}, you can use 'make bin/mc' and move bin/mc to ${PATH}"; exit 1)
	@which bin/sync_diff_inspector || (echo "run 'make bin/sync_diff_inspector' to download it if you need")

check_engine_integration_test:
	./engine/test/utils/check_case.sh
	./engine/test/integration_tests/run_group.sh "check others"

check_dm_integration_test:
	./dm/tests/run_group.sh "check others"

check_cdc_integration_test:
	./tests/integration_tests/run_group.sh check "others"

bin/mc:
	./scripts/download-mc.sh

bin/sync_diff_inspector:
	./scripts/download-sync-diff.sh

define run_engine_unit_test
	@echo "running unit test for packages:" $(1)
	mkdir -p $(ENGINE_TEST_DIR)
	$(FAILPOINT_ENABLE)
	@export log_level=error; \
	$(GOTEST) -timeout 5m -covermode=atomic -coverprofile="$(ENGINE_TEST_DIR)/cov.unit_test.out" $(1) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	$(FAILPOINT_DISABLE)
endef

engine_unit_test_in_verify_ci: check_failpoint_ctl tools/bin/gotestsum tools/bin/gocov tools/bin/gocov-xml
	mkdir -p $(ENGINE_TEST_DIR)
	$(FAILPOINT_ENABLE)
	@export log_level=error; \
	CGO_ENABLED=1 tools/bin/gotestsum --junitfile engine-junit-report.xml -- -v -timeout 5m -p $(P) --race \
	-covermode=atomic -coverprofile="$(ENGINE_TEST_DIR)/cov.unit_test.out" $(ENGINE_PACKAGES) \
	|| { $(FAILPOINT_DISABLE); exit 1; }
	tools/bin/gocov convert "$(ENGINE_TEST_DIR)/cov.unit_test.out" | tools/bin/gocov-xml > engine-coverage.xml
	$(FAILPOINT_DISABLE)

prepare_test_binaries:
	./scripts/download-integration-test-binaries.sh "$(branch)" "$(community)" "$(ver)" "$(os)" "$(arch)"
	touch prepare_test_binaries
