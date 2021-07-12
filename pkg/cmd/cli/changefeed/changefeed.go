package changefeed

import (
	"context"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/cmd/cli/capture"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/clientv3/concurrency"
	"io/ioutil"
	"net/url"
)

type CommonOptions struct {
	changefeedID string
	NoConfirm    bool
}

func NewCommonOptions() *CommonOptions {
	return &CommonOptions{
		changefeedID: "",
		NoConfirm:    false,
	}
}

func NewCmdChangefeed(f util.Factory) *cobra.Command {
	o := NewCommonOptions()
	cmds := &cobra.Command{
		Use:   "changefeed",
		Short: "Manage changefeed (changefeed is a replication task)",
	}

	cmds.AddCommand(NewCmdListChangefeeds(f))
	cmds.AddCommand(NewCmdQueryChangefeed(f, o))
	cmds.AddCommand(NewCmdPauseChangefeed(f, o))
	cmds.AddCommand(NewCmdResumeChangefeed(f, o))
	cmds.AddCommand(NewCmdRemoveChangefeed(f, o))
	cmds.AddCommand(NewCmdCreateChangefeed(f, o))
	cmds.AddCommand(NewCmdUpdateChangefeed(f, o))
	cmds.AddCommand(NewCmdStatisticsChangefeed(f, o))
	cmds.AddCommand(NewCmdCyclicChangefeed(f, o))

	cmds.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	cmds.PersistentFlags().BoolVar(&o.NoConfirm, "no-confirm", false, "Don't ask user whether to ignore ineligible table")

	_ = cmds.MarkPersistentFlagRequired("changefeed-id")
	return cmds
}

func getAllCaptures(f util.Factory, ctx context.Context) ([]*capture.Capture, error) {
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return nil, err
	}
	_, raw, err := etcdClient.GetCaptures(ctx)
	if err != nil {
		return nil, err
	}
	ownerID, err := etcdClient.GetOwnerID(ctx, kv.CaptureOwnerKey)
	if err != nil && errors.Cause(err) != concurrency.ErrElectionNoLeader {
		return nil, err
	}
	captures := make([]*capture.Capture, 0, len(raw))
	for _, c := range raw {
		isOwner := c.ID == ownerID
		captures = append(captures,
			&capture.Capture{ID: c.ID, IsOwner: isOwner, AdvertiseAddr: c.AdvertiseAddr})
	}
	return captures, nil
}

func getOwnerCapture(f util.Factory, ctx context.Context) (*capture.Capture, error) {
	captures, err := getAllCaptures(f, ctx)
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

func ApplyOwnerChangefeedQuery(f util.Factory,
	ctx context.Context, cid model.ChangeFeedID, credential *security.Credential,
) (string, error) {
	owner, err := getOwnerCapture(f, ctx)
	if err != nil {
		return "", err
	}
	scheme := "http"
	if credential.IsTLSEnabled() {
		scheme = "https"
	}
	addr := fmt.Sprintf("%s://%s/capture/owner/changefeed/query", scheme, owner.AdvertiseAddr)
	cli, err := httputil.NewClient(credential)
	if err != nil {
		return "", err
	}
	resp, err := cli.PostForm(addr, url.Values(map[string][]string{
		cdc.APIOpVarChangefeedID: {cid},
	}))
	if err != nil {
		return "", err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", errors.BadRequestf("query changefeed simplified status")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", errors.BadRequestf("%s", string(body))
	}
	return string(body), nil
}

func ApplyAdminChangefeed(f util.Factory, ctx context.Context, job model.AdminJob, credential *security.Credential) error {
	owner, err := getOwnerCapture(f, ctx)
	if err != nil {
		return err
	}
	scheme := "http"
	if credential.IsTLSEnabled() {
		scheme = "https"
	}
	addr := fmt.Sprintf("%s://%s/capture/owner/admin", scheme, owner.AdvertiseAddr)
	cli, err := httputil.NewClient(credential)
	if err != nil {
		return err
	}
	forceRemoveOpt := "false"
	if job.Opts != nil && job.Opts.ForceRemove {
		forceRemoveOpt = "true"
	}
	resp, err := cli.PostForm(addr, map[string][]string{
		cdc.APIOpVarAdminJob:           {fmt.Sprint(int(job.Type))},
		cdc.APIOpVarChangefeedID:       {job.CfID},
		cdc.APIOpForceRemoveChangefeed: {forceRemoveOpt},
	})
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.BadRequestf("admin changefeed failed")
		}
		return errors.BadRequestf("%s", string(body))
	}
	return nil
}
