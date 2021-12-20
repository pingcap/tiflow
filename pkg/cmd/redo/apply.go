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

// applyRedoOptions defines flags for the `redo apply` command.
type applyRedoOptions struct {
	options
	sinkURI string
}

// newapplyRedoOptions creates new applyRedoOptions for the `redo apply` command.
func newapplyRedoOptions() *applyRedoOptions {
	return &applyRedoOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *applyRedoOptions) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&o.sinkURI, "sink-uri", "", "target database sink-uri")
	// the possible error returned from MarkFlagRequired is `no such flag`
	cmd.MarkFlagRequired("sink-uri") //nolint:errcheck
}

// run runs the `redo apply` command.
func (o *applyRedoOptions) run(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()

	cfg := &applier.RedoApplierConfig{
		Storage: o.storage,
		SinkURI: o.sinkURI,
		Dir:     o.dir,
	}
	ap := applier.NewRedoApplier(cfg)
	err := ap.Apply(ctx)
	if err != nil {
		return err
	}
	cmd.Println("Apply redo log successfully")
	return nil
}

// newCmdApply creates the `redo apply` command.
func newCmdApply(opt *options) *cobra.Command {
	o := newapplyRedoOptions()
	command := &cobra.Command{
		Use:   "apply",
		Short: "Apply redo logs in target sink",
		RunE: func(cmd *cobra.Command, args []string) error {
			o.options = *opt
			return o.run(cmd)
		},
	}
	o.addFlags(command)

	return command
}
