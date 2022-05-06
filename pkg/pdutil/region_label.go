// Copyright 2022 PingCAP, Inc.
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

package pdutil

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/pingcap/tiflow/pkg/retry"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	regionLabelPrefix = "/pd/api/v1/config/region-label/rules"

	// Split the default rule by `6e000000000000000000f8` to keep metadata region
	// isolated from the normal data area.
	addMetaJSON = `{
		"sets": [
			{
				"id": "ticdc/meta",
				"labels": [
					{
						"key": "data-type",
						"value": "meta"
					}
				],
				"rule_type": "key-range",
				"data": [
					{
						"start_key": "6d00000000000000f8",
						"end_key": "6e00000000000000f8"
					}
				]
			}
		]
	}`
)

var defaultMaxRetry uint64 = 3

// pdAPIClient is api client of Placement Driver.
type pdAPIClient struct {
	pdClient   pd.Client
	dialClient *httputil.Client
}

// newPDApiClient create a new pdAPIClient.
func newPDApiClient(ctx context.Context, pdClient pd.Client) (*pdAPIClient, error) {
	conf := config.GetGlobalServerConfig()
	dialClient, err := httputil.NewClient(conf.Security)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &pdAPIClient{
		pdClient:   pdClient,
		dialClient: dialClient,
	}, nil
}

// UpdateMetaLabel is a reentrant function that updates the meta-region label of upstream cluster.
func UpdateMetaLabel(ctx context.Context, pdClient pd.Client) error {
	pc, err := newPDApiClient(ctx, pdClient)
	if err != nil {
		return err
	}
	defer pc.dialClient.CloseIdleConnections()

	err = retry.Do(ctx, func() error {
		err = pc.patchMetaLabel(ctx)
		if err != nil {
			log.Error("Fail to add meta region label to PD", zap.Error(err))
			return err
		}

		log.Info("Succeed to add meta region label to PD")
		return nil
	}, retry.WithMaxTries(defaultMaxRetry), retry.WithIsRetryableErr(func(err error) bool {
		switch errors.Cause(err) {
		case context.Canceled:
			return false
		}
		return true
	}))
	return err
}

func (pc *pdAPIClient) patchMetaLabel(ctx context.Context) error {
	url := pc.pdClient.GetLeaderAddr() + regionLabelPrefix
	header := http.Header{"Content-Type": {"application/json"}}
	content := []byte(addMetaJSON)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := pc.dialClient.DoRequest(ctx, url, http.MethodPatch,
		header, bytes.NewReader(content))
	return errors.Trace(err)
}
