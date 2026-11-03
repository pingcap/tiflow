// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestStatisticsReportsErrors(t *testing.T) {
	original := cmdcontext.GetDefaultContext()
	t.Cleanup(func() { cmdcontext.SetDefaultContext(original) })
	for _, stage := range []string{"changefeed", "tso", "poll"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			cmdcontext.SetDefaultContext(ctx)
			f := newMockFactory(gomock.NewController(t))
			o := newStatisticsChangefeedOptions()
			require.NoError(t, o.complete(f))
			o.namespace, o.changefeedID, o.interval = "team-a", "orders", 1
			denied := errors.ErrForbidden.GenWithStackByArgs("reader", "changefeed.read")
			if stage == "poll" {
				gomock.InOrder(
					f.changefeeds.EXPECT().Get(gomock.Any(), "team-a", "orders").Return(&v2.ChangeFeedInfo{UpstreamID: 42}, nil),
					f.tso.EXPECT().Query(gomock.Any(), &v2.UpstreamConfig{ID: 42}).Return(&v2.Tso{}, nil),
					f.changefeeds.EXPECT().Get(gomock.Any(), "team-a", "orders").Return(nil, denied),
				)
			} else if stage == "changefeed" {
				f.changefeeds.EXPECT().Get(gomock.Any(), "team-a", "orders").Return(nil, denied).AnyTimes()
			} else {
				f.changefeeds.EXPECT().Get(gomock.Any(), "team-a", "orders").Return(&v2.ChangeFeedInfo{UpstreamID: 42}, nil).AnyTimes()
				f.tso.EXPECT().Query(gomock.Any(), &v2.UpstreamConfig{ID: 42}).Return(nil, denied).AnyTimes()
			}
			cmd := &cobra.Command{}
			var output bytes.Buffer
			cmd.SetOut(&output)
			err := o.run(cmd)
			require.ErrorIs(t, err, denied)
			if stage == "poll" {
				require.Contains(t, output.String(), "replication_gap")
			}
		})
	}
}
