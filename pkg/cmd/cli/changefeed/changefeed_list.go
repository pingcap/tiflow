package changefeed

import (
	"encoding/json"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// changefeedCommonInfo holds some common used information of a changefeed
type changefeedCommonInfo struct {
	ID      string              `json:"id"`
	Summary *cdc.ChangefeedResp `json:"summary"`
}

type ListChangefeedOptions struct {
	changefeedListAll bool
}

func NewListChangefeedOptions() *ListChangefeedOptions {
	return &ListChangefeedOptions{
		changefeedListAll: false,
	}
}

func NewCmdListChangefeeds(f util.Factory) *cobra.Command {
	o := NewListChangefeedOptions()
	command := &cobra.Command{
		Use:   "list",
		Short: "List all replication tasks (changefeeds) in TiCDC cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.GetDefaultContext()
			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}
			_, raw, err := etcdClient.GetChangeFeeds(ctx)
			if err != nil {
				return err
			}
			changefeedIDs := make(map[string]struct{}, len(raw))
			for id := range raw {
				changefeedIDs[id] = struct{}{}
			}
			if o.changefeedListAll {
				statuses, err := etcdClient.GetAllChangeFeedStatus(ctx)
				if err != nil {
					return err
				}
				for cid := range statuses {
					changefeedIDs[cid] = struct{}{}
				}
			}
			cfs := make([]*changefeedCommonInfo, 0, len(changefeedIDs))
			for id := range changefeedIDs {
				cfci := &changefeedCommonInfo{ID: id}
				resp, err := ApplyOwnerChangefeedQuery(f, ctx, id, f.GetCredential())
				if err != nil {
					// if no capture is available, the query will fail, just add a warning here
					log.Warn("query changefeed info failed", zap.String("error", err.Error()))
				} else {
					info := &cdc.ChangefeedResp{}
					err = json.Unmarshal([]byte(resp), info)
					if err != nil {
						return err
					}
					cfci.Summary = info
				}
				cfs = append(cfs, cfci)
			}
			return util.JsonPrint(cmd, cfs)
		},
	}
	command.PersistentFlags().BoolVarP(&o.changefeedListAll, "all", "a", false, "List all replication tasks(including removed and finished)")

	return command
}
