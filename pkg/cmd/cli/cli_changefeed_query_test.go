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
	"io"
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/api/v2/mock"
	"github.com/stretchr/testify/require"
)

func TestChangefeedQueryCli(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cfV2 := mock.NewMockChangefeedInterface(ctrl)

	f := &mockFactory{changefeeds: cfV2}

	o := newQueryChangefeedOptions()
	o.complete(f)
	cmd := newCmdQueryChangefeed(f)

	cfV2.EXPECT().List(gomock.Any(), gomock.Any(), "all").Return([]v2.ChangefeedCommonInfo{
		{
			UpstreamID:     1,
			Namespace:      "default",
			ID:             "abc",
			CheckpointTime: model.JSONTime{},
			RunningError:   nil,
		},
	}, nil)

	o.simplified = true
	o.changefeedID = "abc"
	require.Nil(t, o.run(cmd))
	cfV2.EXPECT().List(gomock.Any(), gomock.Any(), "all").Return([]v2.ChangefeedCommonInfo{
		{
			UpstreamID:     1,
			Namespace:      "default",
			ID:             "abc",
			CheckpointTime: model.JSONTime{},
			RunningError:   nil,
		},
	}, nil)

	o.simplified = true
	o.changefeedID = "abcd"
	require.NotNil(t, o.run(cmd))

	cfV2.EXPECT().List(gomock.Any(), gomock.Any(), "all").Return(nil, errors.New("test"))
	o.simplified = true
	o.changefeedID = "abcd"
	require.NotNil(t, o.run(cmd))

	// query success
	cfV2.EXPECT().Get(gomock.Any(), gomock.Any(), "bcd").Return(&v2.ChangeFeedInfo{}, nil)

	o.simplified = false
	o.changefeedID = "bcd"
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	require.Nil(t, o.run(cmd))
	out, err := io.ReadAll(b)
	require.Nil(t, err)
	// make sure config is printed
	require.Contains(t, string(out), "config")

	// query failed
	cfV2.EXPECT().Get(gomock.Any(), gomock.Any(), "bcd").Return(nil, errors.New("test"))
	os.Args = []string{"query", "--simple=false", "--changefeed-id=bcd"}
	require.NotNil(t, o.run(cmd))
}
