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

package capture

import (
	"context"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

var _ = check.Suite(&httpValidatorSuite{})

type httpValidatorSuite struct {
}

func (s *httpValidatorSuite) TestVerifyUpdateChangefeedConfig(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	// test startTs > targetTs
	changefeedConfig := model.ChangefeedConfig{TargetTS: 20}
	oldInfo := &model.ChangeFeedInfo{StartTs: 40}
	newInfo, err := verifyUpdateChangefeedConfig(ctx, changefeedConfig, oldInfo)
	c.Assert(err, check.NotNil)
	c.Assert(newInfo, check.IsNil)

	// test no change
	changefeedConfig = model.ChangefeedConfig{SinkURI: "blcakhole://"}
	oldInfo = &model.ChangeFeedInfo{SinkURI: "blcakhole://"}
	newInfo, err = verifyUpdateChangefeedConfig(ctx, changefeedConfig, oldInfo)
	c.Assert(err, check.NotNil)
	c.Assert(newInfo, check.IsNil)

	// test no error
	changefeedConfig = model.ChangefeedConfig{MounterWorkerNum: 32}
	mounterConfig := &config.MounterConfig{WorkerNum: 16}
	replicateConfig := &config.ReplicaConfig{Mounter: mounterConfig}
	oldInfo = &model.ChangeFeedInfo{Config: replicateConfig}
	newInfo, err = verifyUpdateChangefeedConfig(ctx, changefeedConfig, oldInfo)
	c.Assert(err, check.IsNil)
	c.Assert(newInfo, check.NotNil)
}

func (s *httpValidatorSuite) TestVerifySink(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	replicateConfig := &config.ReplicaConfig{}
	opts := make(map[string]string)

	// test sink uri error
	sinkURI := "mysql:127.0.0.1:0000"
	err := verifySink(ctx, sinkURI, replicateConfig, opts)
	c.Assert(err, check.NotNil)

	// test sink uri right
	sinkURI = "blackhole://"
	err = verifySink(ctx, sinkURI, replicateConfig, opts)
	c.Assert(err, check.IsNil)
}
