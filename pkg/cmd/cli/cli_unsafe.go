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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
)

// unsafeCommonOptions defines common for the `cli unsafe` command.
type unsafeCommonOptions struct {
	noConfirm bool
}

// newUnsafeCommonOptions creates new common options for the `cli unsafe` command.
func newUnsafeCommonOptions() *unsafeCommonOptions {
	return &unsafeCommonOptions{
		noConfirm: false,
	}
}

func (o *unsafeCommonOptions) confirmMetaDelete(cmd *cobra.Command) error {
	if o.noConfirm {
		return nil
	}
	cmd.Printf("Confirm that you know what this command will do and use it at your own risk [Y/N]\n")
	var yOrN string
	_, err := fmt.Scan(&yOrN)
	if err != nil {
		return err
	}
	if strings.ToLower(strings.TrimSpace(yOrN)) != "y" {
		return errors.NewNoStackError("abort meta command")
	}
	return nil
}

// newCmdUnsafe creates the `cli unsafe` command.
func newCmdUnsafe(f util.Factory) *cobra.Command {
	commonOptions := newUnsafeCommonOptions()

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
