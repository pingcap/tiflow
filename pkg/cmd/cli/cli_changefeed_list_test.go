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

package cli

import (
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/api/v1/mock"
	"github.com/stretchr/testify/require"
)

func TestChangefeedListCli(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cf := mock.NewMockChangefeedInterface(ctrl)
	f := &mockFactory{changefeeds: cf}
	cmd := newCmdListChangefeed(f)
	cf.EXPECT().List(gomock.Any(), "all").Return(&[]model.ChangefeedCommonInfo{
		{
			UpstreamID:     1,
			Namespace:      "default",
			ID:             "abc",
			CheckpointTime: model.JSONTime{},
			RunningError:   nil,
		},
	}, nil)
	os.Args = []string{"list", "--all=true"}
	require.Nil(t, cmd.Execute())
	os.Args = []string{"list", "--all=false"}
	cf.EXPECT().List(gomock.Any(), "").Return(nil, errors.New("test"))
	require.NotNil(t, cmd.Execute())
}
