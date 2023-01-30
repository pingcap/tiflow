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
	apiv2client "github.com/pingcap/tiflow/pkg/api/v2"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/spf13/cobra"
)

// capture holds capture information.
type capture struct {
	ID            string `json:"id"`
	IsOwner       bool   `json:"is-owner"`
	AdvertiseAddr string `json:"address"`
	ClusterID     string `json:"cluster-id"`
}

// listCaptureOptions defines flags for the `cli capture list` command.
type listCaptureOptions struct {
	apiv2Client apiv2client.APIV2Interface
}

// newListCaptureOptions creates new listCaptureOptions for the `cli capture list` command.
func newListCaptureOptions() *listCaptureOptions {
	return &listCaptureOptions{}
}

// complete adapts from the command line args to the data and client required.
func (o *listCaptureOptions) complete(f factory.Factory) error {
	apiv2Client, err := f.APIV2Client()
	if err != nil {
		return err
	}
	o.apiv2Client = apiv2Client
	return nil
}

// run runs the `cli capture list` command.
func (o *listCaptureOptions) run(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()

	raw, err := o.apiv2Client.Captures().List(ctx)
	if err != nil {
		return err
	}
	captures := make([]*capture, 0, len(raw))
	for _, c := range raw {
		captures = append(captures,
			&capture{
				ID:            c.ID,
				IsOwner:       c.IsOwner,
				AdvertiseAddr: c.AdvertiseAddr,
				ClusterID:     c.ClusterID,
			})
	}

	return util.JSONPrint(cmd, captures)
}

// newCmdListCapture creates the `cli capture list` command.
func newCmdListCapture(f factory.Factory) *cobra.Command {
	o := newListCaptureOptions()

	command := &cobra.Command{
		Use:   "list",
		Short: "List all captures in TiCDC cluster",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			util.CheckErr(o.complete(f))
			util.CheckErr(o.run(cmd))
		},
	}

	return command
}
