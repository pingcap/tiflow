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

package v2

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/cdc/api/validator"
	"github.com/pingcap/tiflow/cdc/kv"
)

// VerifyTable verify table, return ineligibleTables and EligibleTables.
func (h *OpenAPIV2) VerifyTable(c *gin.Context) {
	cfg := defaultVerifyTableConfig()
	err := c.BindJSON(cfg)
	if err != nil {
		_ = c.Error(err)
		return
	}
	kvStore, err := kv.CreateTiStore(strings.Join(cfg.PDAddrs, ","), cfg.Credential)
	if err != nil {
		_ = c.Error(err)
		return
	}
	ineligibleTables, eligibleTables, err := validator.VerifyTables(cfg.ReplicaConfig, kvStore, cfg.StartTs)
	if err != nil {
		_ = c.Error(err)
		return
	}
	tables := &Tables{IneligibleTables: ineligibleTables, EligibleTables: eligibleTables}
	c.JSON(http.StatusOK, tables)
}
