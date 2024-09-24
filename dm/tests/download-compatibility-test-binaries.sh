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

# download-compatibility-test-binaries.sh
# Downloads all the binaries needed for dm compatibility testing
#
# Notice: This script is intended for Linux platforms only.

set -euo pipefail

# Constants
FILE_SERVER_URL="http://fileserver.pingcap.net"
GITHUB_RELEASE_URL="https://github.com/github/gh-ost/releases/download/v1.1.0"
TEMP_DIR="tmp"
THIRD_BIN_DIR="third_bin"
FINAL_BIN_DIR="bin"

# Color output function
color-green() {
	echo -e "\x1B[1;32m${*}\x1B[0m"
}

# Download function
download() {
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

function get_sha1() {
	local repo="$1"
	local branch="$2"
	file_server_url="http://fileserver.pingcap.net"
	sha1=$(curl -s "${file_server_url}/download/refs/pingcap/${repo}/${branch}/sha1")
	if [ $? -ne 0 ] || echo "$sha1" | grep -q "Error"; then
		echo "Failed to get sha1 with repo ${repo} branch ${branch}: $sha1. use branch master to instead" >&2
		branch=master
		sha1=$(curl -s "${file_server_url}/download/refs/pingcap/${repo}/${branch}/sha1")
	fi
	echo $sha1
}

# Extract function
extract() {
	local file_name=$1
	local extract_dir=$2
	local target_file=$3
	tar -xz -C "${extract_dir}" "${target_file}" -f "${TEMP_DIR}/${file_name}"
}

# Main function
main() {
	local default_branch=$1

	# Get SHA1 values, using environment variables if set, otherwise use default_branch
	local tidb_branch=${TIDB_BRANCH:-$default_branch}
	# Get TiDB SHA1
	local tidb_sha1=$(curl "${FILE_SERVER_URL}/download/refs/pingcap/tidb/${tidb_branch}/sha1")

	# Define download URLs
	local download_urls=(
		"${FILE_SERVER_URL}/download/builds/pingcap/tidb/${tidb_sha1}/centos7/tidb-server.tar.gz"
		"http://download.pingcap.org/tidb-enterprise-tools-nightly-linux-amd64.tar.gz"
		"http://download.pingcap.org/tidb-enterprise-tools-latest-linux-amd64.tar.gz"
		"${GITHUB_RELEASE_URL}/gh-ost-binary-linux-20200828140552.tar.gz"
		"${FILE_SERVER_URL}/download/minio.tar.gz"
	)

	# Prepare directories
	rm -rf "$TEMP_DIR" "$THIRD_BIN_DIR"
	mkdir -p "$TEMP_DIR" "$THIRD_BIN_DIR" "$FINAL_BIN_DIR"

	color-green "Downloading binaries..."

	# Download and extract binaries
	for url in "${download_urls[@]}"; do
		local filename=$(basename "$url")
		download "$url" "$filename" "${TEMP_DIR}/${filename}"
		case "$filename" in
		tidb-server.tar.gz)
			extract "$filename" "$THIRD_BIN_DIR" "bin/tidb-server"
			mv "${THIRD_BIN_DIR}/bin/tidb-server" "$THIRD_BIN_DIR/"
			;;
		tidb-enterprise-tools-nightly-linux-amd64.tar.gz)
			extract "$filename" "$THIRD_BIN_DIR" "tidb-enterprise-tools-nightly-linux-amd64/bin/sync_diff_inspector"
			mv "${THIRD_BIN_DIR}/tidb-enterprise-tools-nightly-linux-amd64/bin/sync_diff_inspector" "$THIRD_BIN_DIR/"
			rm -rf "${THIRD_BIN_DIR}/tidb-enterprise-tools-nightly-linux-amd64"
			;;
		tidb-enterprise-tools-latest-linux-amd64.tar.gz)
			extract "$filename" "$THIRD_BIN_DIR" "tidb-enterprise-tools-latest-linux-amd64/bin/mydumper"
			mv "${THIRD_BIN_DIR}/tidb-enterprise-tools-latest-linux-amd64/bin/mydumper" "$THIRD_BIN_DIR/"
			rm -rf "${THIRD_BIN_DIR}/tidb-enterprise-tools-latest-linux-amd64"
			;;
		minio.tar.gz | gh-ost-binary-linux-20200828140552.tar.gz)
			tar -xz -C "$THIRD_BIN_DIR" -f "${TEMP_DIR}/${filename}"
			;;
		esac
	done

	# Set permissions and move files
	chmod a+x "${THIRD_BIN_DIR}"/*
	rm -rf "$TEMP_DIR" "${FINAL_BIN_DIR}/bin"
	mv "${THIRD_BIN_DIR}"/* "${FINAL_BIN_DIR}/"
	rm -rf "$THIRD_BIN_DIR"

	color-green "Download SUCCESS"
}

# Run the main function
main "$1"
