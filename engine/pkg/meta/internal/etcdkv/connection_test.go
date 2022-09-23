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

package etcdkv

import (
	"regexp"
	"testing"

	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestClientConnImpl(t *testing.T) {
	t.Parallel()

	svr, endpoints, err := RetryMockBackendEtcd()
	require.NoError(t, err)
	defer CloseEmbededEtcd(svr)

	// normal case
	cc, err := NewClientConnImpl(&metaModel.StoreConfig{
		StoreType: metaModel.StoreTypeEtcd,
		Endpoints: []string{endpoints},
	})
	require.Nil(t, err)
	defer cc.Close()

	require.Equal(t, metaModel.StoreTypeEtcd, cc.StoreType())
	_, err = cc.GetConn()
	require.NoError(t, err)
	require.Nil(t, cc.Close())
}

func TestClientConnImplError(t *testing.T) {
	t.Parallel()

	_, err := NewClientConnImpl(&metaModel.StoreConfig{
		StoreType: metaModel.StoreTypeMySQL,
	})
	require.Regexp(t, regexp.QuoteMeta("[DFLOW:ErrMetaParamsInvalid]"+
		"etcd conn but get unmatch type"), err.Error())
}
