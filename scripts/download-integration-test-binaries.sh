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
# * download all the binaries you need for integration testing

set -o errexit
set -o pipefail

# Specify which branch to be utilized for executing the test, which is
# exclusively accessible when obtaining binaries from
# http://fileserver.pingcap.net.
branch=${1:-master}
# Specify whether to download the community version of binaries, the following
# four arguments are applicable only when utilizing the community version of
# binaries.
community=${2:-false}
# Specify which version of the community binaries that will be utilized.
ver=${3:-v8.1.0}
# Specify which os that will be used to pack the binaries.
os=${4:-linux}
# Specify which architecture that will be used to pack the binaries.
arch=${5:-amd64}

set -o nounset

# See https://misc.flogisoft.com/bash/tip_colors_and_formatting.
color-green() { # Green
	echo -e "\x1B[1;32m${*}\x1B[0m"
}

function download() {
	local url=$1
	local file_name=$2
	local file_path=$3
	if [[ -f "${file_path}" ]]; then
		echo "file ${file_name} already exists, skip download"
		return
	fi
	echo ">>>"
	echo "download ${file_name} from ${url}"
	wget --no-verbose --retry-connrefused --waitretry=1 -t 3 -O "${file_path}" "${url}"
}

# download_community_version will try to download required binaries from the
# public accessible community version
function download_community_binaries() {
	local dist="${ver}-${os}-${arch}"
	local tidb_file_name="tidb-community-server-$dist"
	local tidb_tar_name="${tidb_file_name}.tar.gz"
	local tidb_url="https://download.pingcap.org/$tidb_tar_name"
	local toolkit_file_name="tidb-community-toolkit-$dist"
	local toolkit_tar_name="${toolkit_file_name}.tar.gz"
	local toolkit_url="https://download.pingcap.org/$toolkit_tar_name"

	color-green "Download community binaries..."
	download "$tidb_url" "$tidb_tar_name" "tmp/$tidb_tar_name"
	download "$toolkit_url" "$toolkit_tar_name" "tmp/$toolkit_tar_name"
	# extract the tidb community version binaries
	tar -xz -C tmp -f tmp/$tidb_tar_name
	# extract the pd server
	tar -xz -C third_bin -f tmp/$tidb_file_name/pd-${dist}.tar.gz
	# extract the tikv server
	tar -xz -C third_bin -f tmp/$tidb_file_name/tikv-${dist}.tar.gz
	# extract the tidb server
	tar -xz -C third_bin -f tmp/$tidb_file_name/tidb-${dist}.tar.gz
	# extract the tiflash
	tar -xz -C third_bin -f tmp/$tidb_file_name/tiflash-${dist}.tar.gz &&
		mv third_bin/tiflash third_bin/_tiflash &&
		mv third_bin/_tiflash/* third_bin && rm -rf third_bin/_tiflash
	# extract the pd-ctl
	tar -xz -C third_bin pd-ctl -f tmp/$tidb_file_name/ctl-${dist}.tar.gz
	# extract the toolkit community version binaries, get the etcdctl and
	# the sync_diff_inspector
	tar -xz -C third_bin \
		$toolkit_file_name/etcdctl $toolkit_file_name/sync_diff_inspector \
		-f tmp/$toolkit_tar_name &&
		mv third_bin/$toolkit_file_name/* third_bin &&
		rm -rf third_bin/$toolkit_file_name

	# ycsb
	local ycsb_file_name="go-ycsb-${os}-${arch}"
	local ycsb_tar_name="${ycsb_file_name}.tar.gz"
	local ycsb_url="https://github.com/pingcap/go-ycsb/releases/download/v1.0.0/${ycsb_tar_name}"
	wget -O "tmp/$ycsb_tar_name" "$ycsb_url"
	tar -xz -C third_bin -f tmp/$ycsb_tar_name

	# minio
	local minio_url="https://dl.min.io/server/minio/release/${os}-${arch}/minio"
	download "$minio_url" "minio" "third_bin/minio"

	# jq
	local os_name=$([ "$os" == "darwin" ] && echo -n "macos" || echo -n "$os")
	local jq_url="https://github.com/jqlang/jq/releases/download/jq-1.7.1/jq-${os_name}-${arch}"
	wget -O third_bin/jq "$jq_url"

	chmod a+x third_bin/*
}

function download_binaries() {
	color-green "Download binaries..."
	# PingCAP file server URL.
	file_server_url="http://fileserver.pingcap.net"

	# Get sha1 based on branch name.
	tidb_sha1=$(curl "${file_server_url}/download/refs/pingcap/tidb/${branch}/sha1")
	tikv_sha1=$(curl "${file_server_url}/download/refs/pingcap/tikv/${branch}/sha1")
	pd_sha1=$(curl "${file_server_url}/download/refs/pingcap/pd/${branch}/sha1")
	tiflash_sha1=$(curl "${file_server_url}/download/refs/pingcap/tiflash/${branch}/sha1")

	# All download links.
	tidb_download_url="${file_server_url}/download/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz"
	tikv_download_url="${file_server_url}/download/builds/pingcap/tikv/${tikv_sha1}/centos7/tikv-server.tar.gz"
	pd_download_url="${file_server_url}/download/builds/pingcap/pd/${pd_sha1}/centos7/pd-server.tar.gz"
	tiflash_download_url="${file_server_url}/download/builds/pingcap/tiflash/${branch}/${tiflash_sha1}/centos7/tiflash.tar.gz"
	minio_download_url="${file_server_url}/download/minio.tar.gz"
	go_ycsb_download_url="${file_server_url}/download/builds/pingcap/go-ycsb/test-br/go-ycsb"
	etcd_download_url="${file_server_url}/download/builds/pingcap/cdc/etcd-v3.4.7-linux-amd64.tar.gz"
	sync_diff_inspector_url="${file_server_url}/download/builds/pingcap/cdc/sync_diff_inspector_hash-79f1fd1e_linux-amd64.tar.gz"
	jq_download_url="${file_server_url}/download/builds/pingcap/test/jq-1.6/jq-linux64"
	schema_registry_url="${file_server_url}/download/builds/pingcap/cdc/schema-registry.tar.gz"

	download "$tidb_download_url" "tidb-server.tar.gz" "tmp/tidb-server.tar.gz"
	tar -xz -C third_bin bin/tidb-server -f tmp/tidb-server.tar.gz && mv third_bin/bin/tidb-server third_bin/

	download "$pd_download_url" "pd-server.tar.gz" "tmp/pd-server.tar.gz"
	tar -xz -C third_bin 'bin/*' -f tmp/pd-server.tar.gz && mv third_bin/bin/* third_bin/

	download "$tikv_download_url" "tikv-server.tar.gz" "tmp/tikv-server.tar.gz"
	tar -xz -C third_bin bin/tikv-server -f tmp/tikv-server.tar.gz && mv third_bin/bin/tikv-server third_bin/

	download "$tiflash_download_url" "tiflash.tar.gz" "tmp/tiflash.tar.gz"
	tar -xz -C third_bin -f tmp/tiflash.tar.gz
	mv third_bin/tiflash third_bin/_tiflash
	mv third_bin/_tiflash/* third_bin && rm -rf third_bin/_tiflash

	download "$minio_download_url" "minio.tar.gz" "tmp/minio.tar.gz"
	tar -xz -C third_bin -f tmp/minio.tar.gz

	download "$go_ycsb_download_url" "go-ycsb" "third_bin/go-ycsb"
	download "$jq_download_url" "jq" "third_bin/jq"
	download "$etcd_download_url" "etcd.tar.gz" "tmp/etcd.tar.gz"
	tar -xz -C third_bin etcd-v3.4.7-linux-amd64/etcdctl -f tmp/etcd.tar.gz
	mv third_bin/etcd-v3.4.7-linux-amd64/etcdctl third_bin/ && rm -rf third_bin/etcd-v3.4.7-linux-amd64

	download "$sync_diff_inspector_url" "sync_diff_inspector.tar.gz" "tmp/sync_diff_inspector.tar.gz"
	tar -xz -C third_bin -f tmp/sync_diff_inspector.tar.gz

	download "$schema_registry_url" "schema-registry.tar.gz" "tmp/schema-registry.tar.gz"
	tar -xz -C third_bin -f tmp/schema-registry.tar.gz
	mv third_bin/schema-registry third_bin/_schema_registry
	mv third_bin/_schema_registry/* third_bin && rm -rf third_bin/_schema_registry

	chmod a+x third_bin/*
}

# Some temporary dir.
rm -rf tmp
rm -rf third_bin

mkdir -p third_bin
mkdir -p tmp
mkdir -p bin

[ $community == true ] && download_community_binaries || download_binaries

# Copy it to the bin directory in the root directory.
rm -rf tmp
rm -rf bin/bin
mv third_bin/* ./bin
rm -rf third_bin

color-green "Download SUCCESS"
