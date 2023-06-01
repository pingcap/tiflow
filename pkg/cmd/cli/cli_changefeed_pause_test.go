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
	"github.com/pingcap/tiflow/pkg/api/v2/mock"
	"github.com/stretchr/testify/require"
)

func TestChangefeedPauseCli(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cf := mock.NewMockChangefeedInterface(ctrl)
	f := &mockFactory{changefeeds: cf}
	cmd := newCmdPauseChangefeed(f)
	cf.EXPECT().Pause(gomock.Any(), "default", "abc").Return(nil)
	os.Args = []string{"pause", "--changefeed-id=abc", "--namespace=default"}
	require.Nil(t, cmd.Execute())

	cf.EXPECT().Pause(gomock.Any(), "test", "abc").Return(errors.New("test"))
	o := newPauseChangefeedOptions()
	o.changefeedID = "abc"
	o.namespace = "test"
	require.Nil(t, o.complete(f))
	require.NotNil(t, o.run())
}
