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
	"strings"
	"time"

	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	apiv2client "github.com/pingcap/tiflow/pkg/api/v2"
	"github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
)

// unsafeResolveLockOptions defines flags for the `cli unsafe show-metadata` command.
type unsafeResolveLockOptions struct {
	apiClient apiv2client.APIV2Interface

	regionID uint64
	ts       uint64

	upstreamPDAddrs  string
	upstreamCaPath   string
	upstreamCertPath string
	upstreamKeyPath  string
}

// newUnsafeResolveLockOptions creates new unsafeResolveLockOptions
// for the `cli unsafe show-metadata` command.
func newUnsafeResolveLockOptions() *unsafeResolveLockOptions {
	return &unsafeResolveLockOptions{}
}

// complete adapts from the command line args to the data and client required.
func (o *unsafeResolveLockOptions) complete(f factory.Factory) error {
	ctx := context.GetDefaultContext()
	apiClient, err := f.APIV2Client()
	if err != nil {
		return err
	}
	o.apiClient = apiClient
	if o.ts == 0 {
		var pdAddrs []string
		if o.upstreamPDAddrs != "" {
			pdAddrs = strings.Split(o.upstreamPDAddrs, ",")
		}
		tso, err := apiClient.Tso().Query(ctx, &v2.UpstreamConfig{
			PDConfig: v2.PDConfig{
				PDAddrs:  pdAddrs,
				CAPath:   o.upstreamCaPath,
				CertPath: o.upstreamCertPath,
				KeyPath:  o.upstreamKeyPath,
			},
		})
		if err != nil {
			return err
		}
		now := oracle.GetTimeFromTS(oracle.ComposeTS(tso.Timestamp, tso.LogicTime))
		// Try not kill active transaction, we only resolves lock 1 minute ago.
		o.ts = oracle.GoTimeToTS(now.Add(-time.Minute))
	}
	return err
}

// run runs the `cli unsafe show-metadata` command.
func (o *unsafeResolveLockOptions) run() error {
	ctx := context.GetDefaultContext()
	var pdAddrs []string
	if o.upstreamPDAddrs != "" {
		pdAddrs = strings.Split(o.upstreamPDAddrs, ",")
	}
	return o.apiClient.Unsafe().ResolveLock(ctx, &v2.ResolveLockReq{
		RegionID: o.regionID,
		Ts:       o.ts,
		PDConfig: v2.PDConfig{
			PDAddrs:  pdAddrs,
			CAPath:   o.upstreamCaPath,
			CertPath: o.upstreamCertPath,
			KeyPath:  o.upstreamKeyPath,
		},
	})
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
	cmd.PersistentFlags().StringVar(&o.upstreamPDAddrs, "upstream-pd", "",
		"upstream PD address, use ',' to separate multiple PDs")
	cmd.PersistentFlags().StringVar(&o.upstreamCaPath, "upstream-ca", "",
		"CA certificate path for TLS connection to upstream")
	cmd.PersistentFlags().StringVar(&o.upstreamCertPath, "upstream-cert", "",
		"Certificate path for TLS connection to upstream")
	cmd.PersistentFlags().StringVar(&o.upstreamKeyPath, "upstream-key", "",
		"Private key path for TLS connection to upstream")
	// we don't support specify there flags below when cdc version <= 6.3.0
	_ = cmd.PersistentFlags().MarkHidden("upstream-pd")
	_ = cmd.PersistentFlags().MarkHidden("upstream-ca")
	_ = cmd.PersistentFlags().MarkHidden("upstream-cert")
	_ = cmd.PersistentFlags().MarkHidden("upstream-key")
}

// newCmdResolveLock creates the `cli unsafe show-metadata` command.
func newCmdResolveLock(f factory.Factory) *cobra.Command {
	o := newUnsafeResolveLockOptions()

	command := &cobra.Command{
		Use:   "resolve-lock",
		Short: "resolve locks in regions",
		Args:  cobra.NoArgs,
		Run: func(_ *cobra.Command, args []string) {
			util.CheckErr(o.complete(f))
			util.CheckErr(o.run())
		},
	}

	o.addFlags(command)

	return command
}
