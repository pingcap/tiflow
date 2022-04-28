// Copyright 2022 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/txnutil"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

// unsafeResolveLockOptions defines flags for the `cli unsafe show-metadata` command.
type unsafeResolveLockOptions struct {
	kvStorage kv.Storage

	regionID uint64
	ts       uint64
}

// newUnsafeResolveLockOptions creates new unsafeResolveLockOptions
// for the `cli unsafe show-metadata` command.
func newUnsafeResolveLockOptions() *unsafeResolveLockOptions {
	return &unsafeResolveLockOptions{}
}

// complete adapts from the command line args to the data and client required.
func (o *unsafeResolveLockOptions) complete(f factory.Factory) error {
	ctx := context.GetDefaultContext()

	pdClient, err := f.PdClient()
	if err != nil {
		return err
	}
	if o.ts == 0 {
		ts, logic, err := pdClient.GetTS(ctx)
		if err != nil {
			return err
		}
		now := oracle.GetTimeFromTS(oracle.ComposeTS(ts, logic))
		// Try not kill active transaction, we only resolves lock 1 minute ago.
		o.ts = oracle.GoTimeToTS(now.Add(-time.Minute))
	}

	o.kvStorage, err = f.KvStorage()
	return err
}

// run runs the `cli unsafe show-metadata` command.
func (o *unsafeResolveLockOptions) run() error {
	ctx := context.GetDefaultContext()

	conf := &log.Config{Level: "info", File: log.FileLogConfig{}}
	lg, p, e := log.InitLogger(conf)
	if e != nil {
		return e
	}
	log.ReplaceGlobals(lg, p)
	txnResolver := txnutil.NewLockerResolver(o.kvStorage.(tikv.Storage),
		model.DefaultChangeFeedID("changefeed-client"),
		util.RoleClient)
	return txnResolver.Resolve(ctx, o.regionID, o.ts)
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *unsafeResolveLockOptions) addFlags(cmd *cobra.Command) {
	if o == nil {
		return
	}

	cmd.Flags().Uint64Var(&o.regionID, "region", 0, "Region ID")
	cmd.Flags().Uint64Var(&o.ts, "ts", 0,
		"resolve locks before the timestamp, default 1 minute ago from now")
	_ = cmd.MarkFlagRequired("region")
}

// newCmdResolveLock creates the `cli unsafe show-metadata` command.
func newCmdResolveLock(f factory.Factory) *cobra.Command {
	o := newUnsafeResolveLockOptions()

	command := &cobra.Command{
		Use:   "resolve-lock",
		Short: "resolve locks in regions",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, args []string) error {
			err := o.complete(f)
			if err != nil {
				return err
			}

			return o.run()
		},
	}

	o.addFlags(command)

	return command
}
