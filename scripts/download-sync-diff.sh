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

echo "will download tidb-tools v6.1.0 to get sync_diff_inspector"
curl -C - --retry 3 -o /tmp/tidb-tools.tar.gz https://download.pingcap.org/tidb-community-toolkit-v6.1.0-linux-amd64.tar.gz
mkdir -p /tmp/tidb-tools
tar -zxf /tmp/tidb-tools.tar.gz -C /tmp/tidb-tools
mv /tmp/tidb-tools/tidb-community-toolkit-v6.1.0-linux-amd64/sync_diff_inspector ./bin/sync_diff_inspector
rm -r /tmp/tidb-tools
rm /tmp/tidb-tools.tar.gz
