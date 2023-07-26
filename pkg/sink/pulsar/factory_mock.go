// Copyright 2023 PingCAP, Inc.
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

package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// NewMockCreatorFactory returns a factory implemented based on kafka-go
func NewMockCreatorFactory(config *Config, changefeedID model.ChangeFeedID) (pulsar.Client, error) {
	log.L().Info("mock pulsar client factory created", zap.Any("changfeedID", changefeedID))
	return nil, nil
}
