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

# download-compatibility-test-binaries.sh will
# * download all the binaries you need for dm compatibility testing

# Notice:
# Please don't try the script locally,
# it downloads files for linux platform.

set -o errexit
set -o nounset
set -o pipefail

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

# Specify the download branch.
branch=$1

# PingCAP file server URL.
file_server_url="http://fileserver.pingcap.net"

# Get sha1 based on branch name.
tidb_sha1=$(curl "${file_server_url}/download/refs/pingcap/tidb/${branch}/sha1")

# All download links.
tidb_download_url="${file_server_url}/download/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz"
sync_diff_inspector_download_url="http://download.pingcap.org/tidb-enterprise-tools-nightly-linux-amd64.tar.gz"
mydumper_download_url="http://download.pingcap.org/tidb-enterprise-tools-latest-linux-amd64.tar.gz"

gh_os_download_url="https://github.com/github/gh-ost/releases/download/v1.1.0/gh-ost-binary-linux-20200828140552.tar.gz"
minio_download_url="${file_server_url}/download/minio.tar.gz"

# Some temporary dir.
rm -rf tmp
rm -rf third_bin

mkdir -p third_bin
mkdir -p tmp
mkdir -p bin

color-green "Download binaries..."
download "$tidb_download_url" "tidb-server.tar.gz" "tmp/tidb-server.tar.gz"
tar -xz -C third_bin bin/tidb-server -f tmp/tidb-server.tar.gz && mv third_bin/bin/tidb-server third_bin/

download "$sync_diff_inspector_download_url" "tidb-enterprise-tools-nightly-linux-amd64.tar.gz" "tmp/tidb-enterprise-tools-nightly-linux-amd64.tar.gz"
tar -xz -C third_bin tidb-enterprise-tools-nightly-linux-amd64/bin/sync_diff_inspector -f tmp/tidb-enterprise-tools-nightly-linux-amd64.tar.gz
mv third_bin/tidb-enterprise-tools-nightly-linux-amd64/bin/sync_diff_inspector third_bin/ && rm -rf third_bin/tidb-enterprise-tools-nightly-linux-amd64
download "$mydumper_download_url" "tidb-enterprise-tools-latest-linux-amd64.tar.gz" "tmp/tidb-enterprise-tools-latest-linux-amd64.tar.gz"
tar -xz -C third_bin tidb-enterprise-tools-latest-linux-amd64/bin/mydumper -f tmp/tidb-enterprise-tools-latest-linux-amd64.tar.gz
mv third_bin/tidb-enterprise-tools-latest-linux-amd64/bin/mydumper third_bin/ && rm -rf third_bin/tidb-enterprise-tools-latest-linux-amd64
download "$minio_download_url" "minio.tar.gz" "tmp/minio.tar.gz"
tar -xz -C third_bin -f tmp/minio.tar.gz
download "$gh_os_download_url" "gh-ost-binary-linux-20200828140552.tar.gz" "tmp/gh-ost-binary-linux-20200828140552.tar.gz"
tar -xz -C third_bin -f tmp/gh-ost-binary-linux-20200828140552.tar.gz

chmod a+x third_bin/*

# Copy it to the bin directory in the root directory.
rm -rf tmp
mv third_bin/* ./bin
rm -rf bin/bin
rm -rf third_bin

color-green "Download SUCCESS"
