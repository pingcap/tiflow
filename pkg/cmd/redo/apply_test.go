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

package redo

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestComplete(t *testing.T) {
	cmd := &cobra.Command{
		Use: "test",
	}
	o := newapplyRedoOptions()
	o.sinkURI = "mysql://root@127.0.0.1:3306?safe-mode=false"
	err := o.complete(cmd)
	require.NoError(t, err)
	require.Equal(t, "mysql://root@127.0.0.1:3306?safe-mode=true", o.sinkURI)

	o.sinkURI = "mysql://root@127.0.0.1:3306"
	err = o.complete(cmd)
	require.NoError(t, err)
	require.Equal(t, "mysql://root@127.0.0.1:3306?safe-mode=true", o.sinkURI)

	o.sinkURI = "mysql://root@127.0.0.1:3306?time-zone=UTC&safe-mode=true"
	err = o.complete(cmd)
	require.NoError(t, err)
	require.Equal(t, "mysql://root@127.0.0.1:3306?time-zone=UTC&safe-mode=true", o.sinkURI)
}
