// Copyright 2024 PingCAP, Inc.
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
	"encoding/json"
	"strings"

	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	apiv2client "github.com/pingcap/tiflow/pkg/api/v2"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/spf13/cobra"
)

// querySafePointOptions defines flags for the `cli safepoint query` command.
type querySafePointOptions struct {
	onlyCDC  bool
	clientV2 apiv2client.APIV2Interface
}

// querySafePointOptions creates new querySafePointOptions for the `cli safepoint query` command.
func newQuerySafePointOptions() *querySafePointOptions {
	return &querySafePointOptions{}
}

func (o *querySafePointOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().BoolVar(&o.onlyCDC, "cdc", false, "show cdc only")
}

func (o *querySafePointOptions) finish(f factory.Factory) error {
	clientV2, err := f.APIV2Client()
	if err != nil {
		return err
	}
	o.clientV2 = clientV2
	return nil
}

// showSafePoints query pd to get all safepoints
func (o *querySafePointOptions) showSafePoints(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()
	safepoint, err := o.clientV2.SafePoint().Query(ctx)
	if err != nil {
		return err
	}
	// TODO: move to cmd/api
	if o.onlyCDC {
		points := make([]*v2.ServiceSafePoint, 0)
		for _, point := range safepoint.ServiceGCSafepoints {
			if strings.HasPrefix(point.ServiceID, "ticdc") {
				points = append(points, point)
			}
		}
		safepoint.ServiceGCSafepoints = points
	}
	data, err := json.MarshalIndent(safepoint, "", "  ")
	if err != nil {
		return err
	}
	cmd.Println(string(data))
	return nil
}

// newCmdQuerySafePoint creates the `cli safepoint query` command.
func newCmdQuerySafePoint(f factory.Factory) *cobra.Command {
	o := newQuerySafePointOptions()
	command := &cobra.Command{
		Use:   "query",
		Short: "Get safepoint",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			util.CheckErr(o.finish(f))
			util.CheckErr(o.showSafePoints(cmd))
		},
	}
	o.addFlags(command)

	return command
}
