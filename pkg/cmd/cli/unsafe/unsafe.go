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

package unsafe

import (
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
)

// commonOptions defines common for the `cli unsafe` command.
type commonOptions struct {
	noConfirm bool
}

// newCommonOptions creates new common options for the `cli unsafe` command.
func newCommonOptions() *commonOptions {
	return &commonOptions{
		noConfirm: false,
	}
}

// NewCmdUnsafe creates the `cli unsafe` command.
func NewCmdUnsafe(f util.Factory) *cobra.Command {
	commonOptions := newCommonOptions()

	command := &cobra.Command{
		Use:    "unsafe",
		Hidden: true,
	}

	command.AddCommand(newCmdReset(f, commonOptions))
	command.AddCommand(newCmdShowMetadata(f))
	command.AddCommand(newCmdDeleteServiceGcSafepoint(f, commonOptions))

	command.PersistentFlags().BoolVar(&commonOptions.noConfirm, "no-confirm", false, "Don't ask user whether to confirm executing meta command")

	return command
}
