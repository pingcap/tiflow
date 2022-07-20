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
	"bytes"
	"io/ioutil"
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
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cf.EXPECT().List(gomock.Any(), gomock.Any()).Return(&[]model.ChangefeedCommonInfo{
		{
			UpstreamID:     1,
			Namespace:      "default",
			ID:             "c1",
			CheckpointTime: model.JSONTime{},
			RunningError:   nil,
			FeedState:      model.StateError,
		},
		{
			UpstreamID:     1,
			Namespace:      "default",
			ID:             "c2",
			CheckpointTime: model.JSONTime{},
			RunningError:   nil,
			FeedState:      model.StateNormal,
		},
		{
			UpstreamID:     1,
			Namespace:      "default",
			ID:             "c3",
			CheckpointTime: model.JSONTime{},
			RunningError:   nil,
			FeedState:      model.StateFailed,
		},
	}, nil)
	os.Args = []string{"list", "--all=true"}
	require.Nil(t, cmd.Execute())
	out, err := ioutil.ReadAll(b)
	require.Nil(t, err)
	// make sure the output contains error state changefeed.
	require.Contains(t, string(out), "c1")
	require.Contains(t, string(out), "c2")

	os.Args = []string{"list", "--all=false"}
	cf.EXPECT().List(gomock.Any(), gomock.Any()).Return(nil, errors.New("test"))
	require.NotNil(t, cmd.Execute())
}
