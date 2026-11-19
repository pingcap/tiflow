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

package api

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	mock_owner "github.com/pingcap/tiflow/cdc/owner/mock"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestIsHTTPBadRequestError(t *testing.T) {
	t.Parallel()
	err := cerror.ErrAPIInvalidParam.GenWithStack("aa")
	require.Equal(t, true, IsHTTPBadRequestError(err))
	err = cerror.ErrAPIInvalidParam.Wrap(errors.New("aa"))
	require.Equal(t, true, IsHTTPBadRequestError(err))
	err = cerror.ErrPDEtcdAPIError.GenWithStack("aa")
	require.Equal(t, false, IsHTTPBadRequestError(err))
	err = nil
	require.Equal(t, false, IsHTTPBadRequestError(err))
}

func TestCollectTaskStatuses(t *testing.T) {
	t.Parallel()
	changefeedID := model.DefaultChangeFeedID("test-cf")

	// success: task statuses are collected per capture.
	ctrl := gomock.NewController(t)
	provider := mock_owner.NewMockStatusProvider(ctrl)
	provider.EXPECT().GetAllTaskStatuses(gomock.Any(), changefeedID).Return(
		map[model.CaptureID]*model.TaskStatus{
			"capture-1": {Tables: map[model.TableID]*model.TableReplicaInfo{
				3508: {}, 3520: {},
			}},
		}, nil)
	taskStatus, err := CollectTaskStatuses(context.Background(), provider, changefeedID)
	require.NoError(t, err)
	require.Len(t, taskStatus, 1)
	require.Equal(t, "capture-1", taskStatus[0].CaptureID)
	require.ElementsMatch(t, []model.TableID{3508, 3520}, taskStatus[0].Tables)

	// a lookup failure is propagated to the caller, which decides whether the
	// task_status can be omitted.
	provider = mock_owner.NewMockStatusProvider(ctrl)
	provider.EXPECT().GetAllTaskStatuses(gomock.Any(), changefeedID).Return(
		nil, cerror.ErrChangeFeedNotExists.GenWithStackByArgs(changefeedID))
	taskStatus, err = CollectTaskStatuses(context.Background(), provider, changefeedID)
	require.Error(t, err)
	require.Nil(t, taskStatus)
}
