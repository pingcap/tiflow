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

package capture

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/kv"
	cmdcontext "github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/clientv3/concurrency"
)

// Capture holds capture information.
type Capture struct {
	ID            string `json:"id"`
	IsOwner       bool   `json:"is-owner"`
	AdvertiseAddr string `json:"address"`
}

// NewCmdListCapture creates the `capture list` command.
func NewCmdListCapture(f util.Factory) *cobra.Command {
	command := &cobra.Command{
		Use:   "list",
		Short: "List all captures in TiCDC cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmdcontext.GetDefaultContext()
			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}

			captures, err := listCaptures(etcdClient, ctx)
			if err != nil {
				return err
			}

			return util.JsonPrint(cmd, captures)
		},
	}
	return command
}

// listCaptures list all the captures from the etcd.
func listCaptures(etcdClient *kv.CDCEtcdClient, ctx context.Context) ([]*Capture, error) {
	_, raw, err := etcdClient.GetCaptures(ctx)
	if err != nil {
		return nil, err
	}

	ownerID, err := etcdClient.GetOwnerID(ctx, kv.CaptureOwnerKey)
	if err != nil && errors.Cause(err) != concurrency.ErrElectionNoLeader {
		return nil, err
	}

	captures := make([]*Capture, 0, len(raw))
	for _, c := range raw {
		isOwner := c.ID == ownerID
		captures = append(captures,
			&Capture{ID: c.ID, IsOwner: isOwner, AdvertiseAddr: c.AdvertiseAddr})
	}

	return captures, nil
}

// GetOwnerCapture returns the owner capture.
func GetOwnerCapture(etcdClient *kv.CDCEtcdClient, ctx context.Context) (*Capture, error) {
	captures, err := listCaptures(etcdClient, ctx)
	if err != nil {
		return nil, err
	}

	for _, c := range captures {
		if c.IsOwner {
			return c, nil
		}
	}

	return nil, errors.Trace(util.ErrOwnerNotFound)
}
