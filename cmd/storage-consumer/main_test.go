// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/leakutil"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	leakutil.SetUpLeakTest(m)
}

func TestSchemaPathKey(t *testing.T) {
	t.Parallel()

	testCases := []string{"partition_table/t/meta/schema_441278015643582475_3560115091.json"}
	for _, tc := range testCases {
		var schemaKey SchemaPathKey
		schemaKey.parseSchemaFilePath(tc)
		fmt.Println(schemaKey)
		key := schemaKey.getKey()
		_ = key
		log.Info("duplicate schema file found",
			zap.String("path", tc),
			zap.Any("schemaKey", schemaKey), zap.Any("dmlkey", key))
	}
}
