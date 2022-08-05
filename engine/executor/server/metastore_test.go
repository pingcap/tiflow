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
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/executor/server/mocks"
	"github.com/pingcap/tiflow/engine/pkg/client"
	metaMock "github.com/pingcap/tiflow/engine/pkg/meta/mock"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
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

	mockServerMasterClient := client.NewMockServerMasterClient(ctrl)
	manager, mockCreator := newMetastoreManagerForTesting(ctrl)
	ctx := context.Background()

	var discoveryStoreParams metaModel.StoreConfig
	discoveryStoreParams.SetEndpoints("embedded-etcd:1234")

	mockServerMasterClient.EXPECT().
		QueryMetaStore(
			gomock.Any(),
			gomock.Eq(&pb.QueryMetaStoreRequest{Tp: pb.StoreType_ServiceDiscovery})).
		Return(&pb.QueryMetaStoreResponse{
			Address: "embedded-etcd:1234", // fake address
		}, nil).Times(1)

	var frameStoreParams metaModel.StoreConfig
	frameStoreParams.SetEndpoints("127.0.0.1:3306")
	frameParamBytes, err := json.Marshal(frameStoreParams)
	require.NoError(t, err)

	mockServerMasterClient.EXPECT().
		QueryMetaStore(
			gomock.Any(),
			gomock.Eq(&pb.QueryMetaStoreRequest{Tp: pb.StoreType_SystemMetaStore})).
		Return(&pb.QueryMetaStoreResponse{
			Address: string(frameParamBytes),
		}, nil).Times(1)

	var businessStoreParams metaModel.StoreConfig
	businessStoreParams.SetEndpoints("127.0.0.1:12345")
	businessParamBytes, err := json.Marshal(businessStoreParams)
	require.NoError(t, err)

	mockServerMasterClient.EXPECT().
		QueryMetaStore(
			gomock.Any(),
			gomock.Eq(&pb.QueryMetaStoreRequest{Tp: pb.StoreType_AppMetaStore})).
		Return(&pb.QueryMetaStoreResponse{
			Address: string(businessParamBytes),
		}, nil).Times(1)

	fakeEtcdCli := clientv3.NewCtxClient(ctx)
	fakeFrameStore, err := pkgOrm.NewMockClient()
	require.NoError(t, err)
	fakeBusinessClientConn := metaMock.NewMockClientConn()

	mockCreator.
		EXPECT().
		CreateEtcdCliForServiceDiscovery(gomock.Any(), gomock.Eq(discoveryStoreParams)).
		Return(fakeEtcdCli, nil)
	mockCreator.
		EXPECT().
		CreateDBClientForFramework(gomock.Any(), gomock.Eq(frameStoreParams)).
		Return(fakeFrameStore, nil)
	mockCreator.
		EXPECT().
		CreateClientConnForBusiness(gomock.Any(), gomock.Eq(businessStoreParams)).
		Return(fakeBusinessClientConn, nil)
	err = manager.Init(ctx, mockServerMasterClient)
	require.NoError(t, err)

	require.True(t, manager.IsInitialized())

	require.Equal(t, fakeEtcdCli, manager.ServiceDiscoveryStore())
	require.Equal(t, fakeFrameStore, manager.FrameworkStore())
	require.Equal(t, fakeBusinessClientConn, manager.BusinessClientConn())

	manager.Close()

	require.Nil(t, manager.ServiceDiscoveryStore())
	require.Nil(t, manager.FrameworkStore())
	require.Nil(t, manager.BusinessClientConn())
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
		manager.BusinessClientConn()
	})
}
