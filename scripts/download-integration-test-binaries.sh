#!/usr/bin/env bash
# Copyright 2023 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

# download-integration-test-binaries.sh will
# * download all the binaries needed for integration testing

set -euo pipefail

# Default values
DEFAULT_BRANCH=${1:-master}

TIDB_BRANCH=${TIDB_BRANCH:-$DEFAULT_BRANCH}
TIKV_BRANCH=${TIKV_BRANCH:-$DEFAULT_BRANCH}
PD_BRANCH=${PD_BRANCH:-$DEFAULT_BRANCH}
TIFLASH_BRANCH=${TIFLASH_BRANCH:-$DEFAULT_BRANCH}

COMMUNITY=${2:-false}
VERSION=${3:-v8.1.0}
OS=${4:-linux}
ARCH=${5:-amd64}

# Constants
FILE_SERVER_URL="http://fileserver.pingcap.net"
TMP_DIR="tmp"
THIRD_BIN_DIR="third_bin"
BIN_DIR="bin"

# ANSI color codes
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Functions
log_green() {
	echo -e "${GREEN}$1${NC}"
}

download_file() {
	local url=$1
	local file_name=$2
	local file_path=$3
	if [[ -f "${file_path}" ]]; then
		echo "File ${file_name} already exists, skipping download"
		return
	fi
	echo ">>> Downloading ${file_name} from ${url}"
	wget --no-verbose --retry-connrefused --waitretry=1 -t 3 -O "${file_path}" "${url}"
}

get_sha1() {
	local repo="$1"
	local branch="$2"
	local sha1=$(curl -s "${FILE_SERVER_URL}/download/refs/pingcap/${repo}/${branch}/sha1")
	if [ $? -ne 0 ] || echo "$sha1" | grep -q "Error"; then
		echo "Failed to get sha1 for ${repo} branch ${branch}: $sha1. Using default branch ${DEFAULT_BRANCH} instead" >&2
		branch=$DEFAULT_BRANCH
		sha1=$(curl -s "${FILE_SERVER_URL}/download/refs/pingcap/${repo}/${branch}/sha1")
	fi
	echo "$branch:$sha1"
}

download_community_binaries() {
	local dist="${VERSION}-${OS}-${ARCH}"
	local tidb_file_name="tidb-community-server-$dist"
	local tidb_tar_name="${tidb_file_name}.tar.gz"
	local tidb_url="https://download.pingcap.org/$tidb_tar_name"
	local toolkit_file_name="tidb-community-toolkit-$dist"
	local toolkit_tar_name="${toolkit_file_name}.tar.gz"
	local toolkit_url="https://download.pingcap.org/$toolkit_tar_name"

	log_green "Downloading community binaries..."
	download_file "$tidb_url" "$tidb_tar_name" "${TMP_DIR}/$tidb_tar_name"
	download_file "$toolkit_url" "$toolkit_tar_name" "${TMP_DIR}/$toolkit_tar_name"

	# Extract binaries
	tar -xz -C ${TMP_DIR} -f ${TMP_DIR}/$tidb_tar_name
	tar -xz -C ${THIRD_BIN_DIR} -f ${TMP_DIR}/$tidb_file_name/pd-${dist}.tar.gz
	tar -xz -C ${THIRD_BIN_DIR} -f ${TMP_DIR}/$tidb_file_name/tikv-${dist}.tar.gz
	tar -xz -C ${THIRD_BIN_DIR} -f ${TMP_DIR}/$tidb_file_name/tidb-${dist}.tar.gz
	tar -xz -C ${THIRD_BIN_DIR} -f ${TMP_DIR}/$tidb_file_name/tiflash-${dist}.tar.gz
	mv ${THIRD_BIN_DIR}/tiflash ${THIRD_BIN_DIR}/_tiflash
	mv ${THIRD_BIN_DIR}/_tiflash/* ${THIRD_BIN_DIR} && rm -rf ${THIRD_BIN_DIR}/_tiflash
	tar -xz -C ${THIRD_BIN_DIR} pd-ctl -f ${TMP_DIR}/$tidb_file_name/ctl-${dist}.tar.gz
	tar -xz -C ${THIRD_BIN_DIR} $toolkit_file_name/etcdctl $toolkit_file_name/sync_diff_inspector -f ${TMP_DIR}/$toolkit_tar_name
	mv ${THIRD_BIN_DIR}/$toolkit_file_name/* ${THIRD_BIN_DIR} && rm -rf ${THIRD_BIN_DIR}/$toolkit_file_name

	# Download additional tools
	download_ycsb
	download_minio
	download_jq

	chmod a+x ${THIRD_BIN_DIR}/*
}

download_ycsb() {
	local ycsb_file_name="go-ycsb-${OS}-${ARCH}"
	local ycsb_tar_name="${ycsb_file_name}.tar.gz"
	local ycsb_url="https://github.com/pingcap/go-ycsb/releases/download/v1.0.0/${ycsb_tar_name}"
	wget -O "${TMP_DIR}/$ycsb_tar_name" "$ycsb_url"
	tar -xz -C ${THIRD_BIN_DIR} -f ${TMP_DIR}/$ycsb_tar_name
}

download_minio() {
	local minio_url="https://dl.min.io/server/minio/release/${OS}-${ARCH}/minio"
	download_file "$minio_url" "minio" "${THIRD_BIN_DIR}/minio"
}

download_jq() {
	local os_name=$([ "$OS" == "darwin" ] && echo -n "macos" || echo -n "$OS")
	local jq_url="https://github.com/jqlang/jq/releases/download/jq-1.7.1/jq-${os_name}-${ARCH}"
	wget -O ${THIRD_BIN_DIR}/jq "$jq_url"
}

download_binaries() {
	log_green "Downloading binaries..."

	# Get sha1 based on branch name
	local tidb_branch_sha1=$(get_sha1 "tidb" "$TIDB_BRANCH")
	local tikv_branch_sha1=$(get_sha1 "tikv" "$TIKV_BRANCH")
	local pd_branch_sha1=$(get_sha1 "pd" "$PD_BRANCH")
	local tiflash_branch_sha1=$(get_sha1 "tiflash" "$TIFLASH_BRANCH")

	local tidb_branch=$(echo "$tidb_branch_sha1" | cut -d':' -f1)
	local tidb_sha1=$(echo "$tidb_branch_sha1" | cut -d':' -f2)
	local tikv_branch=$(echo "$tikv_branch_sha1" | cut -d':' -f1)
	local tikv_sha1=$(echo "$tikv_branch_sha1" | cut -d':' -f2)
	local pd_branch=$(echo "$pd_branch_sha1" | cut -d':' -f1)
	local pd_sha1=$(echo "$pd_branch_sha1" | cut -d':' -f2)
	local tiflash_branch=$(echo "$tiflash_branch_sha1" | cut -d':' -f1)
	local tiflash_sha1=$(echo "$tiflash_branch_sha1" | cut -d':' -f2)

	# Define download URLs
	local tidb_download_url="${FILE_SERVER_URL}/download/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz"
	local tikv_download_url="${FILE_SERVER_URL}/download/builds/pingcap/tikv/${tikv_sha1}/centos7/tikv-server.tar.gz"
	local pd_download_url="${FILE_SERVER_URL}/download/builds/pingcap/pd/${pd_sha1}/centos7/pd-server.tar.gz"
	local tiflash_download_url="${FILE_SERVER_URL}/download/builds/pingcap/tiflash/${tiflash_branch}/${tiflash_sha1}/centos7/tiflash.tar.gz"
	local minio_download_url="${FILE_SERVER_URL}/download/minio.tar.gz"
	local go_ycsb_download_url="${FILE_SERVER_URL}/download/builds/pingcap/go-ycsb/test-br/go-ycsb"
	local etcd_download_url="${FILE_SERVER_URL}/download/builds/pingcap/cdc/etcd-v3.4.7-linux-amd64.tar.gz"
	local sync_diff_inspector_url="${FILE_SERVER_URL}/download/builds/pingcap/cdc/sync_diff_inspector_hash-a129f096_linux-amd64.tar.gz"
	local jq_download_url="${FILE_SERVER_URL}/download/builds/pingcap/test/jq-1.6/jq-linux64"
	local schema_registry_url="${FILE_SERVER_URL}/download/builds/pingcap/cdc/schema-registry.tar.gz"

	# Download and extract binaries
	download_and_extract "$tidb_download_url" "tidb-server.tar.gz" "bin/tidb-server"
	download_and_extract "$pd_download_url" "pd-server.tar.gz" "bin/*"
	download_and_extract "$tikv_download_url" "tikv-server.tar.gz" "bin/tikv-server"
	download_and_extract "$tiflash_download_url" "tiflash.tar.gz"
	download_and_extract "$minio_download_url" "minio.tar.gz"
	download_and_extract "$etcd_download_url" "etcd.tar.gz" "etcd-v3.4.7-linux-amd64/etcdctl"
	download_and_extract "$sync_diff_inspector_url" "sync_diff_inspector.tar.gz"
	download_and_extract "$schema_registry_url" "schema-registry.tar.gz"

	download_file "$go_ycsb_download_url" "go-ycsb" "${THIRD_BIN_DIR}/go-ycsb"
	download_file "$jq_download_url" "jq" "${THIRD_BIN_DIR}/jq"

	chmod a+x ${THIRD_BIN_DIR}/*
}

download_and_extract() {
	local url=$1
	local file_name=$2
	local extract_path=${3:-""}

	download_file "$url" "$file_name" "${TMP_DIR}/$file_name"
	if [ -n "$extract_path" ]; then
		tar -xz -C ${THIRD_BIN_DIR} $extract_path -f ${TMP_DIR}/$file_name
	else
		tar -xz -C ${THIRD_BIN_DIR} -f ${TMP_DIR}/$file_name
	fi

	# Move extracted files if necessary
	case $file_name in
	"tidb-server.tar.gz") mv ${THIRD_BIN_DIR}/bin/tidb-server ${THIRD_BIN_DIR}/ ;;
	"pd-server.tar.gz") mv ${THIRD_BIN_DIR}/bin/* ${THIRD_BIN_DIR}/ ;;
	"tikv-server.tar.gz") mv ${THIRD_BIN_DIR}/bin/tikv-server ${THIRD_BIN_DIR}/ ;;
	"tiflash.tar.gz")
		mv ${THIRD_BIN_DIR}/tiflash ${THIRD_BIN_DIR}/_tiflash
		mv ${THIRD_BIN_DIR}/_tiflash/* ${THIRD_BIN_DIR}/ && rm -rf ${THIRD_BIN_DIR}/_tiflash
		;;
	"etcd.tar.gz")
		mv ${THIRD_BIN_DIR}/etcd-v3.4.7-linux-amd64/etcdctl ${THIRD_BIN_DIR}/
		rm -rf ${THIRD_BIN_DIR}/etcd-v3.4.7-linux-amd64
		;;
	"schema-registry.tar.gz")
		mv ${THIRD_BIN_DIR}/schema-registry ${THIRD_BIN_DIR}/_schema_registry
		mv ${THIRD_BIN_DIR}/_schema_registry/* ${THIRD_BIN_DIR}/ && rm -rf ${THIRD_BIN_DIR}/_schema_registry
		;;
	esac
}

# Main execution
cleanup() {
	rm -rf ${TMP_DIR} ${THIRD_BIN_DIR}
}

setup() {
	cleanup
	mkdir -p ${THIRD_BIN_DIR} ${TMP_DIR} ${BIN_DIR}
}

main() {
	setup

	if [ "$COMMUNITY" = true ]; then
		download_community_binaries
	else
		download_binaries
	fi

	# Move binaries to final location
	mv ${THIRD_BIN_DIR}/* ./${BIN_DIR}

	cleanup
	log_green "Download SUCCESS"
}

main
