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

package meta

import (
	"context"
	"regexp"
	"testing"

	"github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
)

func TestCAError(t *testing.T) {
	conf := model.DefaultStoreConfig()
	conf.Security = &security.Credential{
		CAPath:   "/xxxx.ca",
		CertPath: "/xxx.ce",
	}
	err := CreateSchemaIfNotExists(context.TODO(), *conf)
	require.Error(t, err)
	require.Regexp(t, regexp.QuoteMeta("could not read ca certificate: open"), err)
}
