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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/factory"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

type processorMeta struct {
	Status   *model.TaskStatus   `json:"status"`
	Position *model.TaskPosition `json:"position"`
}

// queryProcessorOptions defines flags for the `cli processor query` command.
type queryProcessorOptions struct {
	etcdClient *kv.CDCEtcdClient

	credential *security.Credential

	changefeedID string
	captureID    string
}

// newQueryProcessorOptions creates new options for the `cli changefeed query` command.
func newQueryProcessorOptions() *queryProcessorOptions {
	return &queryProcessorOptions{}
}

// complete adapts from the command line args to the data and client required.
func (o *queryProcessorOptions) complete(f factory.Factory) error {
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return err
	}

	o.etcdClient = etcdClient
	o.credential = f.GetCredential()

	return nil
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *queryProcessorOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	cmd.PersistentFlags().StringVarP(&o.captureID, "capture-id", "p", "", "capture ID")
	_ = cmd.MarkPersistentFlagRequired("changefeed-id")
	_ = cmd.MarkPersistentFlagRequired("capture-id")
}

// run runs the `cli processor query` command.
func (o *queryProcessorOptions) run(cmd *cobra.Command) error {
	ctx := context.GetDefaultContext()
	owner, err := getOwnerCapture(ctx, o.etcdClient)
	if err != nil {
		return errors.Trace(err)
	}

	scheme := util.HTTP
	if o.credential.IsTLSEnabled() {
		scheme = util.HTTPS
	}

	uri := fmt.Sprintf("%s://%s/api/v1/processors/%s/%s",
		scheme,
		owner.AdvertiseAddr,
		url.QueryEscape(o.changefeedID),
		url.QueryEscape(o.captureID))
	httpClient, err := httputil.NewClient(o.credential)
	if err != nil {
		return errors.Trace(err)
	}
	resp, err := httpClient.Get(uri)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := ioutil.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if err != nil {
			return cerror.ErrCliHttpError.Wrap(err)
		}
		log.Warn("CDC server returned error when handing a query about changefeed status",
			zap.ByteString("body", body))
		return cerror.ErrCliUnexpectedHttpStatus.GenWithStackByArgs(uri, resp.StatusCode)
	}

	var processorDetail model.ProcessorDetail
	if err := json.Unmarshal(body, &processorDetail); err != nil {
		return errors.Trace(err)
	}

	taskStatuses, err := sendOwnerTaskStatusQuery(ctx, o.etcdClient, o.changefeedID, o.credential)
	if err != nil {
		return errors.Trace(err)
	}

	var taskStatus *model.TaskStatus
	for _, status := range taskStatuses {
		if status.CaptureID == o.captureID {
			taskStatus = status.ToCoreTaskStatus()
			break
		}
	}

	if taskStatus == nil {
		// TODO better error handling
		return cerror.ErrTaskStatusNotExists.GenWithStackByArgs(o.captureID)
	}

	meta := &processorMeta{Status: taskStatus, Position: &model.TaskPosition{
		CheckPointTs: processorDetail.CheckPointTs,
		ResolvedTs:   processorDetail.ResolvedTs,
		Error:        processorDetail.Error,
	}}

	return util.JSONPrint(cmd, meta)
}

// newCmdQueryProcessor creates the `cli processor query` command.
func newCmdQueryProcessor(f factory.Factory) *cobra.Command {
	o := newQueryProcessorOptions()

	command := &cobra.Command{
		Use:   "query",
		Short: "Query information and status of a sub replication task (processor)",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := o.complete(f)
			if err != nil {
				return err
			}

			return o.run(cmd)
		},
	}

	o.addFlags(command)

	return command
}
