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
	"github.com/pingcap/ticdc/pkg/applier"
	cmdcontext "github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/spf13/cobra"
)

// metaOptions defines flags for the `redo meta` command.
type metaOptions struct {
	options
	storage string
	dir     string
	s3URI   string
}

// newMetaOptions creates new MetaOptions for the `redo apply` command.
func newMetaOptions() *metaOptions {
	return &metaOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *metaOptions) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&o.storage, "storage", "", "storage of redo log")
	cmd.Flags().StringVar(&o.dir, "dir", "", "local path of redo log")
	cmd.Flags().StringVar(&o.s3URI, "s3-uri", "", "s3 uri of redo log")
	// the possible error returned from MarkFlagRequired is `no such flag`
	cmd.MarkFlagRequired("storage")  //nolint:errcheck
	cmd.MarkFlagRequired("sink-uri") //nolint:errcheck
}

// run runs the `redo apply` command.
func (o *metaOptions) run(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()

	cfg := &applier.RedoApplierConfig{
		Storage: o.storage,
		Dir:     o.dir,
		S3URI:   o.s3URI,
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
func newCmdMeta() *cobra.Command {
	o := newMetaOptions()
	command := &cobra.Command{
		Use:   "meta",
		Short: "read redo log meta",
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.run(cmd)
		},
	}
	o.options.addFlags(command)
	o.addFlags(command)

	return command
}
