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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/api/validator"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
)

func (h *OpenAPIV2) VerifyTable(c *gin.Context) {
	cfg := &VerifyTableConfig{
		Credential:    &security.Credential{},
		ReplicaConfig: config.GetDefaultReplicaConfig(),
	}
	err := c.BindJSON(cfg)
	if err != nil {
		_ = c.Error(err)
		return
	}
	log.Info("cfg", zap.Any("cfg", cfg))
	log.Info("cfg", zap.Strings("pdAddr", cfg.PDAddrs), zap.Uint64("startTs", cfg.StartTs))
	log.Info("pdaddrs", zap.String("pd", strings.Join(cfg.PDAddrs, ",")))
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
