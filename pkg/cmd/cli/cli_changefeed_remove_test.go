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
	"github.com/pingcap/tiflow/pkg/api/v2/mock"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestChangefeedRemoveCli(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cf := mock.NewMockChangefeedInterface(ctrl)
	f := &mockFactory{changefeeds: cf}

	cmd := newCmdRemoveChangefeed(f)

	cf.EXPECT().Get(gomock.Any(), "test", "abc").Return(&v2.ChangeFeedInfo{}, nil)
	cf.EXPECT().Delete(gomock.Any(), "test", "abc").Return(nil)
	cf.EXPECT().Get(gomock.Any(), "test", "abc").Return(nil,
		cerror.ErrChangeFeedNotExists.GenWithStackByArgs("abc"))
	os.Args = []string{"remove", "--changefeed-id=abc", "--namespace=test"}
	require.Nil(t, cmd.Execute())
	cf.EXPECT().Get(gomock.Any(), "default", "abc").Return(nil,
		cerror.ErrChangeFeedNotExists.GenWithStackByArgs("abc"))
	os.Args = []string{"remove", "--changefeed-id=abc", "--namespace=default"}
	require.Nil(t, cmd.Execute())

	o := newRemoveChangefeedOptions()
	o.complete(f)
	o.changefeedID = "abc"
	o.namespace = "test"
	cf.EXPECT().Get(gomock.Any(), "test", "abc").Return(nil, errors.New("abc"))
	require.NotNil(t, o.run(cmd))
}
