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
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestChangefeedResumeCli(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	f := newMockFactory(ctrl)
	o := newResumeChangefeedOptions()
	o.complete(f)
	cmd := newCmdResumeChangefeed(f)

	// 1. test changefeed resume with non-nil changefeed get result, non-nil tso get result
	f.changefeeds.EXPECT().Get(gomock.Any(), "abc").Return(&model.ChangefeedDetail{
		UpstreamID:     1,
		Namespace:      "default",
		ID:             "abc",
		CheckpointTime: model.JSONTime{},
		RunningError:   nil,
	}, nil)
	f.tso.EXPECT().Query(gomock.Any(), gomock.Any()).Return(&v2.Tso{
		Timestamp: time.Now().Unix() * 1000,
	}, nil).AnyTimes()
	f.changefeedsv2.EXPECT().Resume(gomock.Any(), &v2.ResumeChangefeedConfig{
		OverwriteCheckpointTs: 0,
	}, "abc").Return(nil)
	os.Args = []string{"resume", "--no-confirm=true", "--changefeed-id=abc"}
	require.Nil(t, cmd.Execute())

	// 2. test changefeed resume with nil changfeed get result
	f.changefeeds.EXPECT().Get(gomock.Any(), "abc").Return(&model.ChangefeedDetail{}, nil)
	os.Args = []string{"resume", "--no-confirm=false", "--changefeed-id=abc"}
	o.noConfirm = false
	o.changefeedID = "abc"
	require.NotNil(t, o.run(cmd))

	// 3. test changefeed resume with nil tso get result
	f.changefeeds.EXPECT().Get(gomock.Any(), "abc").Return(&model.ChangefeedDetail{
		UpstreamID:     1,
		Namespace:      "default",
		ID:             "abc",
		CheckpointTime: model.JSONTime{},
		CheckpointTSO:  2,
	}, nil)
	f.tso.EXPECT().Query(gomock.Any(), gomock.Any()).Return(nil, errors.New("test")).AnyTimes()
	require.NotNil(t, o.run(cmd))

	// 4. test changefeed resume with non-nil changefeed result, non-nil tso get result,
	// and confirmation checking
	f.changefeeds.EXPECT().Get(gomock.Any(), "abc").Return(&model.ChangefeedDetail{
		UpstreamID:     1,
		Namespace:      "default",
		ID:             "abc",
		CheckpointTime: model.JSONTime{},
		CheckpointTSO:  2,
	}, nil)
	f.tso.EXPECT().Query(gomock.Any(), gomock.Any()).Return(&v2.Tso{
		Timestamp: time.Now().Unix() * 1000,
	}, nil).AnyTimes()
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
	err = o.run(cmd)
	require.NotNil(t, err)
	require.Regexp(t, "cli changefeed resume", err)
}

func TestChangefeedResumeWithNewCheckpointTs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	f := newMockFactory(ctrl)
	o := newResumeChangefeedOptions()
	o.complete(f)
	cmd := newCmdResumeChangefeed(f)

	// 1. test changefeed resume with valid overwritten checkpointTs
	f.changefeeds.EXPECT().Get(gomock.Any(), "abc").Return(&model.ChangefeedDetail{
		UpstreamID:     1,
		Namespace:      "default",
		ID:             "abc",
		CheckpointTime: model.JSONTime{},
		RunningError:   nil,
	}, nil)
	tso := &v2.Tso{
		Timestamp: time.Now().Unix() * 1000,
	}
	f.tso.EXPECT().Query(gomock.Any(), gomock.Any()).Return(tso, nil).AnyTimes()
	f.changefeedsv2.EXPECT().Resume(gomock.Any(), &v2.ResumeChangefeedConfig{
		OverwriteCheckpointTs: oracle.ComposeTS(tso.Timestamp, tso.LogicTime),
	}, "abc").Return(nil)
	os.Args = []string{
		"resume", "--no-confirm=true", "--changefeed-id=abc",
		"--overwrite-checkpoint-ts=now",
	}
	require.Nil(t, cmd.Execute())

	// 2. test changefeed resume with invalid overwritten checkpointTs
	f.changefeeds.EXPECT().Get(gomock.Any(), "abc").Return(&model.ChangefeedDetail{
		UpstreamID:     1,
		Namespace:      "default",
		ID:             "abc",
		CheckpointTime: model.JSONTime{},
		RunningError:   nil,
	}, nil)
	f.tso.EXPECT().Query(gomock.Any(), gomock.Any()).Return(tso, nil).AnyTimes()
	o.noConfirm = true
	o.changefeedID = "abc"
	o.overwriteCheckpointTs = "Hello"
	require.NotNil(t, o.run(cmd))

	// 3. test changefeed resume with checkpointTs larger than current tso
	f.changefeeds.EXPECT().Get(gomock.Any(), "abc").Return(&model.ChangefeedDetail{
		UpstreamID:     1,
		Namespace:      "default",
		ID:             "abc",
		CheckpointTime: model.JSONTime{},
		RunningError:   nil,
	}, nil)
	f.tso.EXPECT().Query(gomock.Any(), gomock.Any()).Return(tso, nil).AnyTimes()
	o.overwriteCheckpointTs = "18446744073709551615"
	require.NotNil(t, o.run(cmd))

	// 4. test changefeed resume with checkpointTs smaller than gcSafePoint
	f.changefeeds.EXPECT().Get(gomock.Any(), "abc").Return(&model.ChangefeedDetail{
		UpstreamID:     1,
		Namespace:      "default",
		ID:             "abc",
		CheckpointTime: model.JSONTime{},
		RunningError:   nil,
	}, nil)
	tso = &v2.Tso{
		Timestamp: 1,
	}
	f.tso.EXPECT().Query(gomock.Any(), gomock.Any()).Return(tso, nil).AnyTimes()
	f.changefeedsv2.EXPECT().Resume(gomock.Any(), &v2.ResumeChangefeedConfig{
		OverwriteCheckpointTs: 262144,
	}, "abc").
		Return(cerror.ErrStartTsBeforeGC)
	o.overwriteCheckpointTs = "262144"
	require.NotNil(t, o.run(cmd))
}
