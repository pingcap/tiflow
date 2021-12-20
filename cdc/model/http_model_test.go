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

package model

import (
	"encoding/json"
	"testing"

	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestChangefeedCommonInfoMarshalJSON(t *testing.T) {
	runningErr := &RunningError{
		"",
		string(cerror.ErrProcessorUnknown.RFCCode()),
		cerror.ErrProcessorUnknown.GetMsg(),
	}
	cfInfo := &ChangefeedCommonInfo{
		ID:           "test",
		FeedState:    StateNormal,
		RunningError: runningErr,
	}
	// when state is normal, the error code is not exist
	cfInfoJSON, err := json.Marshal(cfInfo)
	require.Nil(t, err)
	require.NotContains(t, string(cfInfoJSON), string(cerror.ErrProcessorUnknown.RFCCode()))

	// when state is not normal, the error code is exist
	cfInfo.FeedState = StateError
	cfInfoJSON, err = json.Marshal(cfInfo)
	require.Nil(t, err)
	require.Contains(t, string(cfInfoJSON), string(cerror.ErrProcessorUnknown.RFCCode()))
}

func TestChangefeedDetailMarshalJSON(t *testing.T) {
	runningErr := &RunningError{
		"",
		string(cerror.ErrProcessorUnknown.RFCCode()),
		cerror.ErrProcessorUnknown.GetMsg(),
	}
	cfDetail := &ChangefeedDetail{
		ID:           "test",
		FeedState:    StateNormal,
		RunningError: runningErr,
	}
	// when state is normal, the error code is not exist
	cfInfoJSON, err := json.Marshal(cfDetail)
	require.Nil(t, err)
	require.NotContains(t, string(cfInfoJSON), string(cerror.ErrProcessorUnknown.RFCCode()))

	// when state is not normal, the error code is exist
	cfDetail.FeedState = StateError
	cfInfoJSON, err = json.Marshal(cfDetail)
	require.Nil(t, err)
	require.Contains(t, string(cfInfoJSON), string(cerror.ErrProcessorUnknown.RFCCode()))
}
