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
	"go.uber.org/atomic"
)

func TestClientConnImpl(t *testing.T) {
	t.Parallel()

	svr, endpoints, err := RetryMockBackendEtcd()
	require.NoError(t, err)
	defer CloseEmbededEtcd(svr)

	// normal case
	cc := NewClientConnImpl()
	require.NotNil(t, cc)
	defer cc.Close()

	err = cc.Initialize(&metaModel.StoreConfig{
		StoreType: metaModel.StoreTypeEtcd,
		Endpoints: []string{endpoints},
	})
	require.NoError(t, err)
	require.Equal(t, metaModel.EtcdKVClientType, cc.ClientType())
	_, err = cc.GetConn()
	require.NoError(t, err)
	err = cc.Initialize(&metaModel.StoreConfig{
		StoreType: metaModel.StoreTypeEtcd,
	})
	require.Regexp(t, regexp.QuoteMeta("already initialized"), err.Error())
	require.Nil(t, cc.Close())
	require.Equal(t, atomic.NewBool(false), cc.isInitialized)
}

func TestClientConnImplError(t *testing.T) {
	t.Parallel()

	cc := NewClientConnImpl()
	require.NotNil(t, cc)

	// abnormal cases
	err := cc.Initialize(&metaModel.StoreConfig{
		StoreType: metaModel.StoreTypeSQL,
	})
	require.Regexp(t, regexp.QuoteMeta("[DFLOW:ErrMetaParamsInvalid]"+
		"meta params invalid:etcd conn but get unmatch type"), err.Error())

	err = cc.Initialize(&metaModel.StoreConfig{
		StoreType: metaModel.StoreTypeEtcd,
	})
	require.Regexp(t, regexp.QuoteMeta("[DFLOW:ErrMetaNewClientFail]create meta client fail: "+
		"etcdclient: no available endpoints"), err.Error())

	_, err = cc.GetConn()
	require.Regexp(t, regexp.QuoteMeta("connection is uninitialized"), err.Error())
}
