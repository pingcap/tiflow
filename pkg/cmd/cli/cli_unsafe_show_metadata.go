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
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/spf13/cobra"
)

// unsafeShowMetadataOptions defines flags for the `cli unsafe show-metadata` command.
type unsafeShowMetadataOptions struct {
	etcdClient *etcd.CDCEtcdClient
}

// newUnsafeShowMetadataOptions creates new unsafeShowMetadataOptions
// for the `cli unsafe show-metadata` command.
func newUnsafeShowMetadataOptions() *unsafeShowMetadataOptions {
	return &unsafeShowMetadataOptions{}
}

// complete adapts from the command line args to the data and client required.
func (o *unsafeShowMetadataOptions) complete(f factory.Factory) error {
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return err
	}

	o.etcdClient = etcdClient

	return nil
}

// run runs the `cli unsafe show-metadata` command.
func (o *unsafeShowMetadataOptions) run(cmd *cobra.Command) error {
	ctx := context.GetDefaultContext()

	kvs, err := o.etcdClient.GetAllCDCInfo(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	for _, kv := range kvs {
		cmd.Printf("Key: %s, Value: %s\n", string(kv.Key), string(kv.Value))
	}
	cmd.Printf("Show %d KVs\n", len(kvs))

	return nil
}

// newCmdShowMetadata creates the `cli unsafe show-metadata` command.
func newCmdShowMetadata(f factory.Factory) *cobra.Command {
	o := newUnsafeShowMetadataOptions()

	command := &cobra.Command{
		Use:   "show-metadata",
		Short: "Show metadata stored in PD",
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
