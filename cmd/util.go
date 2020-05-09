package cmd

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"go.etcd.io/etcd/clientv3/concurrency"
)

func getAllCaptures(ctx context.Context) ([]*capture, error) {
	_, raw, err := cdcEtcdCli.GetCaptures(ctx)
	if err != nil {
		return nil, err
	}
	ownerID, err := cdcEtcdCli.GetOwnerID(ctx, kv.CaptureOwnerKey)
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

func getOwnerCapture(ctx context.Context) (*capture, error) {
	captures, err := getAllCaptures(ctx)
	if err != nil {
		return nil, err
	}
	for _, c := range captures {
		if c.IsOwner {
			return c, nil
		}
	}
	return nil, errors.NotFoundf("owner")
}

func applyAdminChangefeed(ctx context.Context, job model.AdminJob) error {
	owner, err := getOwnerCapture(ctx)
	if err != nil {
		return err
	}
	addr := fmt.Sprintf("http://%s/capture/owner/admin", owner.AdvertiseAddr)
	resp, err := http.PostForm(addr, url.Values(map[string][]string{
		cdc.APIOpVarAdminJob:     {fmt.Sprint(int(job.Type))},
		cdc.APIOpVarChangefeedID: {job.CfID},
	}))
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
