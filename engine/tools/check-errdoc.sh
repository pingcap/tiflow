#!/bin/bash
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

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ENGINE_BASE_DIR=$CUR/..

cp $ENGINE_BASE_DIR/errors.toml /tmp/errors.toml.before
$ENGINE_BASE_DIR/../tools/bin/errdoc-gen --source . --module github.com/pingcap/tiflow --output $ENGINE_BASE_DIR/errors.toml --ignore proto,dm,deployments,cdc,pkg,engine/enginepb
diff -q $ENGINE_BASE_DIR/errors.toml /tmp/errors.toml.before
