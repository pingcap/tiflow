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
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/api/v1/mock"
	v2mock "github.com/pingcap/tiflow/pkg/api/v2/mock"
	"github.com/stretchr/testify/require"
)

func TestChangefeedResumeCli(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cf := mock.NewMockChangefeedInterface(ctrl)
	tso := v2mock.NewMockTsoInterface(ctrl)
	f := &mockFactory{changefeeds: cf, tso: tso}
	cmd := newCmdResumeChangefeed(f)
	cf.EXPECT().Resume(gomock.Any(), "abc").Return(nil)
	os.Args = []string{"remove", "--no-confirm=true", "--changefeed-id=abc"}
	require.Nil(t, cmd.Execute())

	cf.EXPECT().List(gomock.Any(), "all").Return(&[]model.ChangefeedCommonInfo{
		{
			UpstreamID:     1,
			Namespace:      "default",
			ID:             "abcd",
			CheckpointTime: model.JSONTime{},
			CheckpointTSO:  2,
		},
	}, nil)
	os.Args = []string{"remove", "--no-confirm=false", "--changefeed-id=abc"}
	require.NotNil(t, cmd.Execute())

	cf.EXPECT().List(gomock.Any(), "all").Return(&[]model.ChangefeedCommonInfo{
		{
			UpstreamID:     1,
			Namespace:      "default",
			ID:             "abc",
			CheckpointTime: model.JSONTime{},
			CheckpointTSO:  2,
		},
	}, nil)
	tso.EXPECT().Query(gomock.Any(), gomock.Any()).Return(nil, errors.New("test"))
	os.Args = []string{"remove", "--no-confirm=false", "--changefeed-id=abc"}
	require.NotNil(t, cmd.Execute())

	cf.EXPECT().List(gomock.Any(), "all").Return(&[]model.ChangefeedCommonInfo{
		{
			UpstreamID:     1,
			Namespace:      "default",
			ID:             "abc",
			CheckpointTime: model.JSONTime{},
			CheckpointTSO:  2,
		},
	}, nil)
	tso.EXPECT().Query(gomock.Any(), gomock.Any()).Return(&v2.Tso{
		Timestamp: time.Now().Unix() * 1000,
	}, nil)
	os.Args = []string{"remove", "--no-confirm=false", "--changefeed-id=abc"}

	dir := t.TempDir()
	path := filepath.Join(dir, "confirm.txt")
	err := os.WriteFile(path, []byte("n"), 0o644)
	require.Nil(t, err)
	file, err := os.Open(path)
	require.Nil(t, err)
	stdin := os.Stdin
	os.Stdin = file
	defer func() {
		os.Stdin = stdin
	}()
	err = cmd.Execute()
	require.NotNil(t, err)
	require.Regexp(t, "abort changefeed create or resume", err)
}
