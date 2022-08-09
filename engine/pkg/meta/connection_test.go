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

package meta

import (
	"regexp"
	"testing"

	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestNewClientConn(t *testing.T) {
	t.Parallel()

	_, err := NewClientConn(&metaModel.StoreConfig{StoreType: "unknown"})
	require.Regexp(t, regexp.QuoteMeta("[DFLOW:ErrMetaClientTypeNotSupport]"+
		"meta client type not support:unknown-kvclient"), err.Error())

	_, err = NewClientConn(&metaModel.StoreConfig{StoreType: metaModel.StoreTypeEtcd})
	require.Regexp(t, regexp.QuoteMeta("[DFLOW:ErrMetaNewClientFail]create meta client fail: "+
		"etcdclient: no available endpoints"), err.Error())
}
