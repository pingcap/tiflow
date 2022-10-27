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
	"context"
	"strconv"
	"strings"

	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	"github.com/pingcap/tiflow/cdc/model"
	apiv1client "github.com/pingcap/tiflow/pkg/api/v1"
	apiv2client "github.com/pingcap/tiflow/pkg/api/v2"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
)

// resumeChangefeedOptions defines flags for the `cli changefeed resume` command.
type resumeChangefeedOptions struct {
	apiV1Client apiv1client.APIV1Interface
	apiV2Client apiv2client.APIV2Interface

	changefeedID          string
	changefeedDetail      *model.ChangefeedDetail
	noConfirm             bool
	overwriteCheckpointTs string
	currentTso            *v2.Tso
	checkpointTs          uint64

	upstreamPDAddrs  string
	upstreamCaPath   string
	upstreamCertPath string
	upstreamKeyPath  string
}

// newResumeChangefeedOptions creates new options for the `cli changefeed pause` command.
func newResumeChangefeedOptions() *resumeChangefeedOptions {
	return &resumeChangefeedOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *resumeChangefeedOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	cmd.PersistentFlags().BoolVar(&o.noConfirm, "no-confirm", false, "Don't ask user whether to ignore ineligible table")
	cmd.PersistentFlags().StringVar(&o.overwriteCheckpointTs, "overwrite-checkpoint-ts", "",
		"Overwrite the changefeed checkpoint ts, should be 'now' or a specified tso value")
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

	_ = cmd.MarkPersistentFlagRequired("changefeed-id")
}

// complete adapts from the command line args to the data and client required.
func (o *resumeChangefeedOptions) complete(f factory.Factory) error {
	apiClient, err := f.APIV1Client()
	if err != nil {
		return err
	}
	o.apiV1Client = apiClient
	apiClient2, err := f.APIV2Client()
	if err != nil {
		return err
	}
	o.apiV2Client = apiClient2
	return nil
}

func (o *resumeChangefeedOptions) getUpstreamConfig() *v2.UpstreamConfig {
	var (
		pdAddrs  []string
		caPath   string
		keyPath  string
		certPath string
	)
	if o.upstreamPDAddrs != "" {
		pdAddrs = strings.Split(o.upstreamPDAddrs, ",")
		caPath = o.upstreamCaPath
		certPath = o.upstreamCertPath
		keyPath = o.upstreamKeyPath
	}
	return &v2.UpstreamConfig{
		PDConfig: v2.PDConfig{
			PDAddrs:       pdAddrs,
			CAPath:        caPath,
			CertPath:      certPath,
			KeyPath:       keyPath,
			CertAllowedCN: nil,
		},
	}
}

func (o *resumeChangefeedOptions) getResumeChangefeedConfig() *v2.ResumeChangefeedConfig {
	upstreamConfig := o.getUpstreamConfig()
	return &v2.ResumeChangefeedConfig{
		OverwriteCheckpointTs: o.checkpointTs,
		PDConfig:              upstreamConfig.PDConfig,
	}
}

func (o *resumeChangefeedOptions) getTSO(ctx context.Context) (*v2.Tso, error) {
	tso, err := o.apiV2Client.Tso().Query(ctx,
		&v2.UpstreamConfig{ID: o.changefeedDetail.UpstreamID})
	if err != nil {
		return nil, err
	}

	return tso, nil
}

func (o *resumeChangefeedOptions) getChangefeedInfo(ctx context.Context) (
	*model.ChangefeedDetail, error,
) {
	detail, err := o.apiV1Client.Changefeeds().Get(ctx, o.changefeedID)
	if err != nil {
		return nil, err
	}

	return detail, nil
}

// confirmResumeChangefeedCheck prompts the user to confirm the use of a large data gap when noConfirm is turned off.
func (o *resumeChangefeedOptions) confirmResumeChangefeedCheck(cmd *cobra.Command) error {
	if !o.noConfirm {
		if len(o.overwriteCheckpointTs) == 0 {
			return confirmLargeDataGap(cmd, o.currentTso.Timestamp,
				o.changefeedDetail.CheckpointTSO, "resume")
		}

		return confirmOverwriteCheckpointTs(cmd, o.changefeedID, o.checkpointTs)
	}
	return nil
}

func (o *resumeChangefeedOptions) validateParams(ctx context.Context) error {
	// check whether the changefeed to be resumed is existing
	detail, err := o.getChangefeedInfo(ctx)
	if err != nil {
		return err
	}
	o.changefeedDetail = detail

	tso, err := o.getTSO(ctx)
	if err != nil {
		return err
	}
	o.currentTso = tso

	if len(o.overwriteCheckpointTs) == 0 {
		return nil
	}

	// validate the --overwrite-checkpoint-ts parameter
	if strings.ToLower(o.overwriteCheckpointTs) == "now" {
		o.checkpointTs = oracle.ComposeTS(tso.Timestamp, tso.LogicTime)
		return nil
	}

	checkpointTs, err := strconv.ParseUint(o.overwriteCheckpointTs, 10, 64)
	if err != nil {
		return cerror.ErrCliInvalidCheckpointTs.GenWithStackByArgs(o.overwriteCheckpointTs)
	}

	if checkpointTs == 0 {
		return cerror.ErrCliInvalidCheckpointTs.GenWithStackByArgs(o.overwriteCheckpointTs)
	}

	if checkpointTs > oracle.ComposeTS(tso.Timestamp, tso.LogicTime) {
		return cerror.ErrCliCheckpointTsIsInFuture.GenWithStackByArgs(checkpointTs)
	}

	o.checkpointTs = checkpointTs
	return nil
}

// run the `cli changefeed resume` command.
func (o *resumeChangefeedOptions) run(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()

	if err := o.validateParams(ctx); err != nil {
		return err
	}

	cfg := o.getResumeChangefeedConfig()
	if err := o.confirmResumeChangefeedCheck(cmd); err != nil {
		return err
	}
	err := o.apiV2Client.Changefeeds().Resume(ctx, cfg, o.changefeedID)

	return err
}

// newCmdResumeChangefeed creates the `cli changefeed resume` command.
func newCmdResumeChangefeed(f factory.Factory) *cobra.Command {
	o := newResumeChangefeedOptions()

	command := &cobra.Command{
		Use:   "resume",
		Short: "Resume a paused replication task (changefeed)",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			util.CheckErr(o.complete(f))
			util.CheckErr(o.run(cmd))
		},
	}

	o.addFlags(command)

	return command
}
