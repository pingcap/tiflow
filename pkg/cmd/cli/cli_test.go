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

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestCliCmdNoArgs(t *testing.T) {
	t.Parallel()

	cmd := NewCmdCli()
	// Only for test.
	cmd.RunE = func(_ *cobra.Command, _ []string) error {
		return nil
	}
	// There is a DBC space before the flag pd.
	flags := []string{"--log-level=info", "　--pd="}
	os.Args = flags
	err := cmd.Execute()
	require.NotNil(t, err)
	require.Regexp(t, ".*unknown command.*u3000--pd.*for.*cli.*", err)

	// There is an unknown args "aa".
	flags = []string{"--log-level=info", "aa"}
	os.Args = flags
	err = cmd.Execute()
	require.NotNil(t, err)
	require.Regexp(t, ".*unknown command.*aa.*for.*cli.*", err)
}
