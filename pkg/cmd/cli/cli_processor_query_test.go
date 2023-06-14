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
	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	"github.com/stretchr/testify/require"
)

func TestProcessorQueryCli(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	f := newMockFactory(ctrl)
	o := newQueryProcessorOptions()
	o.complete(f)
	cmd := newCmdQueryProcessor(f)

	f.processors.EXPECT().Get(gomock.Any(), gomock.Any(), "a", "b").
		Return(nil, errors.New("test"))
	o.changefeedID = "a"
	o.captureID = "b"
	require.NotNil(t, o.run(cmd))

	cmd = newCmdQueryProcessor(f)
	os.Args = []string{"query", "-c", "a", "-p", "b"}
	f.processors.EXPECT().Get(gomock.Any(), gomock.Any(), "a", "b").
		Return(&v2.ProcessorDetail{
			Tables: []int64{1, 2},
		}, nil)
	require.Nil(t, cmd.Execute())
}
