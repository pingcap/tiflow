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

package owner

import (
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/config"
)

var _ = check.Suite(&schemaSuite{})

type schemaSuite struct {
}

func (s *schemaSuite) TestAllPhysicalTables(c *check.C) {
	schema,err:=newSchemaWrap4Owner(nil,0,config.GetDefaultReplicaConfig())
	c.Assert(err,check.IsNil)
	c.Assert(schema.AllPhysicalTables(),check.HasLen,0)
}
