// Copyright 2021 PingCAP, Inc.
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

package factory

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	v2 "github.com/pingcap/tiflow/pkg/api/v2"
	"github.com/pingcap/tiflow/pkg/api/v2/mock"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	mock_factory "github.com/pingcap/tiflow/pkg/cmd/factory/mock"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestFactoryImplPdClient(t *testing.T) {
	t.Parallel()
	c := mock_factory.NewMockClientGetter(gomock.NewController(t))
	f := factoryImpl{ClientGetter: c}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmdcontext.SetDefaultContext(ctx)

	var certAllowedCN []string
	credential := &security.Credential{CertAllowedCN: certAllowedCN}
	c.EXPECT().GetCredential().Return(credential).Times(2)
	grpcTLSOption, err := credential.ToGRPCDialOption()
	require.NoError(t, err)
	c.EXPECT().ToGRPCDialOption().Return(grpcTLSOption, nil).Times(2)

	pdAddr := ""
	c.EXPECT().GetPdAddr().Return(pdAddr).Times(1)
	pdClient, err := f.PdClient()
	require.Nil(t, pdClient)
	require.Contains(t, err.Error(), "ErrInvalidServerOption")

	pdAddr = "http://127.0.0.1:10000,https://127.0.0.1:10001"
	c.EXPECT().GetPdAddr().Return(pdAddr).Times(1)
	pdClient, err = f.PdClient()
	require.Nil(t, pdClient)
	require.Contains(t, err.Error(), "please provide certificate")

	pdAddr = "https://127.0.0.1:10000,https://127.0.0.1:10001"
	credential = &security.Credential{
		CAPath:   "../../../tests/integration_tests/_certificates/ca.pem",
		CertPath: "../../../tests/integration_tests/_certificates/server.pem",
		KeyPath:  "../../../tests/integration_tests/_certificates/server-key.pem",
	}
	c.EXPECT().ToGRPCDialOption().DoAndReturn(func() (grpc.DialOption, error) {
		grpcTLSOption, err = credential.ToGRPCDialOption()
		require.NoError(t, err)
		return grpcTLSOption, nil
	}).Times(1)
	c.EXPECT().GetCredential().Return(credential).Times(1)
	c.EXPECT().GetPdAddr().Return(pdAddr).Times(1)
	pdClient, err = f.PdClient()
	require.Nil(t, pdClient)
	require.Contains(t, err.Error(), "Fail to open PD client")
}

type mockAPIClient struct {
	v2.APIV2Interface
	status v2.StatusInterface
}

func (c *mockAPIClient) Status() v2.StatusInterface {
	return c.status
}

func TestCheckCDCVersion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	status := mock.NewMockStatusInterface(ctrl)
	client := &mockAPIClient{status: status}
	status.EXPECT().Get(gomock.Any()).Return(nil, errors.New("test"))
	require.NotNil(t, checkCDCVersion(client))
	status.EXPECT().Get(gomock.Any()).Return(&model.ServerStatus{
		Version: "",
	}, nil)
	require.NotNil(t, checkCDCVersion(client))
	status.EXPECT().Get(gomock.Any()).Return(&model.ServerStatus{
		Version: "b6.2.0",
	}, nil)
	require.NotNil(t, checkCDCVersion(client))
	status.EXPECT().Get(gomock.Any()).Return(&model.ServerStatus{
		Version: "v6.2.0-master",
	}, nil)
	require.Nil(t, checkCDCVersion(client))
	status.EXPECT().Get(gomock.Any()).Return(&model.ServerStatus{
		Version: "v6.2.0-rc1",
	}, nil)
	require.Nil(t, checkCDCVersion(client))
	status.EXPECT().Get(gomock.Any()).Return(&model.ServerStatus{
		Version: "v6.3.0",
	}, nil)
	require.Nil(t, checkCDCVersion(client))
	status.EXPECT().Get(gomock.Any()).Return(&model.ServerStatus{
		Version: "v6.1.0",
	}, nil)
	err := checkCDCVersion(client)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "the cluster version is too old")
}
