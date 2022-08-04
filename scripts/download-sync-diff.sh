#!/usr/bin/env bash
# Copyright 2022 PingCAP, Inc.
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

set -eu

SHA1=$(curl http://fileserver.pingcap.net/download/refs/pingcap/tidb-tools/master/sha1)
echo "will download tidb-tools at ${SHA1} to get sync_diff_inspector"
curl -C - --retry 3 -o /tmp/tidb-tools.tar.gz http://fileserver.pingcap.net/download/builds/pingcap/tidb-tools/${SHA1}/centos7/tidb-tools.tar.gz
mkdir -p /tmp/tidb-tools
tar -zxf /tmp/tidb-tools.tar.gz -C /tmp/tidb-tools
mv /tmp/tidb-tools/bin/sync_diff_inspector ./bin/sync_diff_inspector
rm -r /tmp/tidb-tools
rm /tmp/tidb-tools.tar.gz
