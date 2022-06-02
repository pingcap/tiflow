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

package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/tiflow/engine/client"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/executor/server/mocks"
	kvMock "github.com/pingcap/tiflow/engine/pkg/meta/kvclient/mock"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
)

func newMetastoreManagerForTesting(ctrl *gomock.Controller) (*metastoreManagerImpl, *mocks.MockMetastoreCreator) {
	mockCreator := mocks.NewMockMetastoreCreator(ctrl)
	return &metastoreManagerImpl{
		creator: mockCreator,
	}, mockCreator
}

func TestMetastoreManagerBasics(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	mockServerMasterClient := &client.MockServerMasterClient{}
	manager, mockCreator := newMetastoreManagerForTesting(ctrl)
	ctx := context.Background()

	mockServerMasterClient.On(
		"QueryMetaStore",
		mock.Anything,
		&pb.QueryMetaStoreRequest{Tp: pb.StoreType_ServiceDiscovery},
		fetchMetastoreConfigTimeout,
	).
		Return(&pb.QueryMetaStoreResponse{
			Address: "embedded-etcd:1234", // fake address
		}, nil).Once()

	var frameStoreParams metaclient.StoreConfigParams
	frameStoreParams.SetEndpoints("127.0.0.1:3306")
	frameParamBytes, err := json.Marshal(frameStoreParams)
	require.NoError(t, err)

	mockServerMasterClient.On(
		"QueryMetaStore",
		mock.Anything,
		&pb.QueryMetaStoreRequest{Tp: pb.StoreType_SystemMetaStore},
		fetchMetastoreConfigTimeout,
	).
		Return(&pb.QueryMetaStoreResponse{
			Address: string(frameParamBytes),
		}, nil).Once()

	var businessStoreParams metaclient.StoreConfigParams
	businessStoreParams.SetEndpoints("127.0.0.1:12345")
	businessParamBytes, err := json.Marshal(businessStoreParams)
	require.NoError(t, err)

	mockServerMasterClient.On(
		"QueryMetaStore",
		mock.Anything,
		&pb.QueryMetaStoreRequest{Tp: pb.StoreType_AppMetaStore},
		fetchMetastoreConfigTimeout,
	).
		Return(&pb.QueryMetaStoreResponse{
			Address: string(businessParamBytes),
		}, nil).Once()

	fakeEtcdCli := clientv3.NewCtxClient(ctx)
	fakeFrameStore, err := pkgOrm.NewMockClient()
	require.NoError(t, err)
	fakeBusinessStore := kvMock.NewMetaMock()

	mockCreator.
		EXPECT().
		CreateEtcdCliForServiceDiscovery(gomock.Any(), gomock.Eq("embedded-etcd:1234")).
		Return(fakeEtcdCli, nil)
	mockCreator.
		EXPECT().
		CreateDBClientForFramework(gomock.Any(), gomock.Eq(frameStoreParams)).
		Return(fakeFrameStore, nil)
	mockCreator.
		EXPECT().
		CreateMetaKVClientForBusiness(gomock.Any(), gomock.Eq(businessStoreParams)).
		Return(fakeBusinessStore, nil)
	err = manager.Init(ctx, mockServerMasterClient)
	require.NoError(t, err)

	require.True(t, manager.IsInitialized())

	require.Equal(t, fakeEtcdCli, manager.ServiceDiscoveryStore())
	require.Equal(t, fakeFrameStore, manager.FrameworkStore())
	require.Equal(t, fakeBusinessStore, manager.BusinessStore())

	manager.Close()

	require.Nil(t, manager.ServiceDiscoveryStore())
	require.Nil(t, manager.FrameworkStore())
	require.Nil(t, manager.BusinessStore())
}

func TestMetastoreManagerUseBeforeInit(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	manager, _ := newMetastoreManagerForTesting(ctrl)
	require.False(t, manager.IsInitialized())

	require.Panics(t, func() {
		manager.ServiceDiscoveryStore()
	})
	require.Panics(t, func() {
		manager.FrameworkStore()
	})
	require.Panics(t, func() {
		manager.BusinessStore()
	})
}
