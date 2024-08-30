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
	"net/http"
	_ "net/http/pprof" //nolint:gosec
	"net/url"
	"runtime/debug"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/applier"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// applyRedoOptions defines flags for the `redo apply` command.
type applyRedoOptions struct {
	options
	sinkURI              string
	enableProfiling      bool
	memoryLimitInGiBytes int64
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
	cmd.Flags().BoolVar(&o.enableProfiling, "enable-profiling", true, "enable pprof profiling")
	cmd.Flags().Int64Var(&o.memoryLimitInGiBytes, "memory-limit", 10, "memory limit in GiB")
}

//nolint:unparam
func (o *applyRedoOptions) complete(cmd *cobra.Command) error {
	// parse sinkURI as a URI
	sinkURI, err := url.Parse(o.sinkURI)
	if err != nil {
		return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	rawQuery := sinkURI.Query()
	// set safe-mode to true if not set
	if rawQuery.Get("safe-mode") != "true" {
		rawQuery.Set("safe-mode", "true")
		sinkURI.RawQuery = rawQuery.Encode()
		o.sinkURI = sinkURI.String()
	}

	totalMemory, err := util.GetMemoryLimit()
	if err == nil {
		totalMemoryInBytes := int64(float64(totalMemory) * 0.8)
		memoryLimitInBytes := o.memoryLimitInGiBytes * 1024 * 1024 * 1024
		if totalMemoryInBytes != 0 && memoryLimitInBytes > totalMemoryInBytes {
			memoryLimitInBytes = totalMemoryInBytes
		}
		debug.SetMemoryLimit(memoryLimitInBytes)
		log.Info("set memory limit", zap.Int64("memoryLimit", memoryLimitInBytes))
	}

	return nil
}

// run runs the `redo apply` command.
func (o *applyRedoOptions) run(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()

	if o.enableProfiling {
		go func() {
			server := &http.Server{
				Addr:              ":6060",
				ReadHeaderTimeout: 5 * time.Second,
			}
			log.Info("Start http pprof server", zap.String("addr", server.Addr))
			if err := server.ListenAndServe(); err != nil {
				log.Fatal("http pprof", zap.Error(err))
			}
		}()
	}

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
			if err := o.complete(cmd); err != nil {
				return err
			}
			return o.run(cmd)
		},
	}
	o.addFlags(command)

	return command
}
