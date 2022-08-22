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
	"strings"

	"github.com/pingcap/errors"
	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	apiv2client "github.com/pingcap/tiflow/pkg/api/v2"
	"github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/spf13/cobra"
)

// unsafeDeleteServiceGcSafepointOptions defines flags
// for the `cli unsafe delete-service-gc-safepoint` command.
type unsafeDeleteServiceGcSafepointOptions struct {
	apiClient        apiv2client.APIV2Interface
	upstreamPDAddrs  string
	upstreamCaPath   string
	upstreamCertPath string
	upstreamKeyPath  string
}

// newUnsafeDeleteServiceGcSafepointOptions creates new unsafeDeleteServiceGcSafepointOptions
// for the `cli unsafe delete-service-gc-safepoint` command.
func newUnsafeDeleteServiceGcSafepointOptions() *unsafeDeleteServiceGcSafepointOptions {
	return &unsafeDeleteServiceGcSafepointOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *unsafeDeleteServiceGcSafepointOptions) addFlags(cmd *cobra.Command) {
	if o == nil {
		return
	}
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

// complete adapts from the command line args to the data and client required.
func (o *unsafeDeleteServiceGcSafepointOptions) complete(f factory.Factory) error {
	apiClient, err := f.APIV2Client()
	if err != nil {
		return err
	}
	o.apiClient = apiClient
	return err
}

// run runs the `cli unsafe delete-service-gc-safepoint` command.
func (o *unsafeDeleteServiceGcSafepointOptions) run(cmd *cobra.Command) error {
	ctx := context.GetDefaultContext()

	err := o.apiClient.Unsafe().DeleteServiceGcSafePoint(ctx, o.getUpstreamConfig())
	if err == nil {
		cmd.Println("CDC service GC safepoint truncated in PD!")
	}

	return errors.Trace(err)
}

func (o *unsafeDeleteServiceGcSafepointOptions) getUpstreamConfig() *v2.UpstreamConfig {
	var pdAddrs []string
	if o.upstreamPDAddrs != "" {
		pdAddrs = strings.Split(o.upstreamPDAddrs, ",")
	}
	return &v2.UpstreamConfig{
		PDConfig: v2.PDConfig{
			PDAddrs:       pdAddrs,
			CAPath:        o.upstreamCaPath,
			CertPath:      o.upstreamCertPath,
			KeyPath:       o.upstreamKeyPath,
			CertAllowedCN: nil,
		},
	}
}

// newCmdDeleteServiceGcSafepoint creates the `cli unsafe delete-service-gc-safepoint` command.
func newCmdDeleteServiceGcSafepoint(f factory.Factory, commonOptions *unsafeCommonOptions) *cobra.Command {
	o := newUnsafeDeleteServiceGcSafepointOptions()

	command := &cobra.Command{
		Use:   "delete-service-gc-safepoint",
		Short: "Delete CDC service GC safepoint in PD, confirm that you know what this command will do and use it at your own risk",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			util.CheckErr(commonOptions.confirmMetaDelete(cmd))
			util.CheckErr(o.complete(f))
			util.CheckErr(o.run(cmd))
		},
	}
	o.addFlags(command)
	return command
}
