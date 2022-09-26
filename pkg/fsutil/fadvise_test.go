// Copyright 2022 PingCAP, Inc.
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
package fsutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestDropPageCache(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "file.test")

	err := os.WriteFile(path, []byte("hello world"), 0o600)
	require.Nil(t, err)

	err = DropPageCache(path)
	require.Nil(t, err)
}

func TestEncodeBinaryToMaxwell(t *testing.T) {
	t.Parallel()

	column := &model.Column{
		Name: "varbinary", Type: mysql.TypeVarchar, Value: []uint8("测试varbinary"),
		Flag: model.BinaryFlag,
	}

	e := &model.RowChangedEvent{
		Table:   &model.TableName{Schema: "a", Table: "b"},
		Columns: []*model.Column{column},
	}

	key, msg := rowChangeToMaxwellMsg(e)
	require.NotNil(t, key)
	require.NotNil(t, msg)
}
