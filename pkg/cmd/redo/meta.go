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
	"github.com/pingcap/tiflow/pkg/applier"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/spf13/cobra"
)

// metaOptions defines flags for the `redo meta` command.
type metaOptions struct {
	options
}

// newMetaOptions creates new MetaOptions for the `redo apply` command.
func newMetaOptions() *metaOptions {
	return &metaOptions{}
}

// run runs the `redo meta` command.
func (o *metaOptions) run(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()

	cfg := &applier.RedoApplierConfig{
		Storage: o.storage,
		Dir:     o.dir,
	}
	ap := applier.NewRedoApplier(cfg)
	checkpointTs, resolvedTs, err := ap.ReadMeta(ctx)
	if err != nil {
		return err
	}
	cmd.Printf("checkpoint-ts:%d, resolved-ts:%d\n", checkpointTs, resolvedTs)
	return nil
}

// newCmdMeta creates the `redo meta` command.
func newCmdMeta(opt *options) *cobra.Command {
	command := &cobra.Command{
		Use:   "meta",
		Short: "read redo log meta",
		RunE: func(cmd *cobra.Command, args []string) error {
			o := newMetaOptions()
			o.options = *opt
			return o.run(cmd)
		},
	}

	return command
}
