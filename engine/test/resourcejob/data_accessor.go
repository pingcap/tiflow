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

package resourcejob

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
)

type masterDAO struct {
	client   metaclient.KVClient
	masterID frameModel.MasterID
}

func newMasterDAO(innerClient metaclient.KVClient, masterID frameModel.MasterID) *masterDAO {
	return &masterDAO{
		client:   innerClient,
		masterID: masterID,
	}
}

func (dao *masterDAO) PutMasterMeta(ctx context.Context, status *masterStatus) error {
	statusBytes, err := json.Marshal(status)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = dao.client.Put(ctx, masterMetaKey(dao.masterID), string(statusBytes))
	if err != nil {
		return err
	}
	return nil
}

func (dao *masterDAO) GetMasterMeta(ctx context.Context, statusOut *masterStatus) error {
	resp, err := dao.client.Get(ctx, masterMetaKey(dao.masterID))
	if err != nil {
		return errors.Trace(err)
	}

	if len(resp.Kvs) != 1 {
		return errors.Trace(errors.Errorf("master meta not found, master-id: %s", dao.masterID))
	}

	statusBytes := resp.Kvs[0].Value
	if err := json.Unmarshal(statusBytes, statusOut); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func masterMetaKey(masterID frameModel.MasterID) string {
	return fmt.Sprintf("/resource-job/master/%s", masterID)
}
