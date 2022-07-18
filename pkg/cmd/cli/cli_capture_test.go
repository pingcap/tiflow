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

	"github.com/stretchr/testify/require"
)

func TestCaptureCli(t *testing.T) {
	cmd := newCmdCapture(nil)
	os.Args = []string{"capture", "--disable-version-check=true"}
	require.Nil(t, cmd.Execute())
	os.Args = []string{"capture", "--disable-version-check=false"}
	require.Nil(t, cmd.Execute())
}
