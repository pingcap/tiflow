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

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/client/v3/concurrency"

	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
)

// capture holds capture information.
type capture struct {
	ID            string `json:"id"`
	IsOwner       bool   `json:"is-owner"`
	AdvertiseAddr string `json:"address"`
}

// listCaptureOptions defines flags for the `cli capture list` command.
type listCaptureOptions struct {
	etcdClient *etcd.CDCEtcdClient
}

// newListCaptureOptions creates new listCaptureOptions for the `cli capture list` command.
func newListCaptureOptions() *listCaptureOptions {
	return &listCaptureOptions{}
}

// complete adapts from the command line args to the data and client required.
func (o *listCaptureOptions) complete(f factory.Factory) error {
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return err
	}

	o.etcdClient = etcdClient

	return nil
}

// run runs the `cli capture list` command.
func (o *listCaptureOptions) run(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()

	captures, err := listCaptures(ctx, o.etcdClient)
	if err != nil {
		return err
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
		RunE: func(cmd *cobra.Command, args []string) error {
			err := o.complete(f)
			if err != nil {
				return err
			}

			return o.run(cmd)
		},
	}

	return command
}

// listCaptures list all the captures from the etcd.
func listCaptures(ctx context.Context, etcdClient *etcd.CDCEtcdClient) ([]*capture, error) {
	_, raw, err := etcdClient.GetCaptures(ctx)
	if err != nil {
		return nil, err
	}

	ownerID, err := etcdClient.GetOwnerID(ctx, etcd.CaptureOwnerKey)
	if err != nil && errors.Cause(err) != concurrency.ErrElectionNoLeader {
		return nil, err
	}

	captures := make([]*capture, 0, len(raw))
	for _, c := range raw {
		isOwner := c.ID == ownerID
		captures = append(captures,
			&capture{ID: c.ID, IsOwner: isOwner, AdvertiseAddr: c.AdvertiseAddr})
	}

	return captures, nil
}

// getOwnerCapture returns the owner capture.
func getOwnerCapture(ctx context.Context, etcdClient *etcd.CDCEtcdClient) (*capture, error) {
	captures, err := listCaptures(ctx, etcdClient)
	if err != nil {
		return nil, err
	}

	for _, c := range captures {
		if c.IsOwner {
			return c, nil
		}
	}

	return nil, errors.Trace(cerror.ErrOwnerNotFound)
}
