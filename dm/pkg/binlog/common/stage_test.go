// Copyright 2019 PingCAP, Inc.
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

package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStageString(t *testing.T) {
	cases := []struct {
		stage Stage
		str   string
	}{
		{
			stage: StageNew,
			str:   "new",
		},
		{
			stage: StagePrepared,
			str:   "prepared",
		},
		{
			stage: StageClosed,
			str:   "closed",
		},
		{
			stage: Stage(100),
			str:   "unknown",
		},
	}

	for _, cs := range cases {
		require.Equal(t, cs.str, cs.stage.String())
	}
}
