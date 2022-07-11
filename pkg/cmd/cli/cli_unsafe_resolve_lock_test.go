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
	"github.com/stretchr/testify/require"
)

func TestUnsafeResolveLockCli(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	f := newMockFactory(ctrl)

	cmd := newCmdResolveLock(f)
	os.Args = []string{
		"resolve",
		"--region=1",
		"--ts=1",
		"--upstream-pd=pd",
		"--upstream-ca=ca",
		"--upstream-cert=cer",
		"--upstream-key=key",
	}
	f.unsafes.EXPECT().ResolveLock(gomock.Any(), gomock.Any()).Return(nil)
	require.Nil(t, cmd.Execute())
}
