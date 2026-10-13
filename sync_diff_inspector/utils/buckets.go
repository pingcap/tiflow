// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	dmlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/sync_diff_inspector/logutil"
	"go.uber.org/zap"
)

// AnalyzeValuesFromBuckets preserves TiDB's bucket parsing while redacting its
// timestamp decoding logs according to the inspector's redaction setting.
func AnalyzeValuesFromBuckets(valueString string, cols []*model.ColumnInfo) ([]string, error) {
	values := strings.Split(strings.Trim(valueString, "()"), ", ")
	if len(values) != len(cols) {
		return nil, errors.Errorf("analyze value %s failed", valueString)
	}

	for i, col := range cols {
		if !dbutil.IsTimeTypeAndNeedDecode(col.GetType()) {
			continue
		}
		// Histograms can contain either formatted dates or packed time values.
		if _, err := types.ParseTime(types.DefaultStmtNoWarningContext, values[i], col.GetType(), types.MinFsp); err == nil {
			continue
		}
		value, err := dbutil.DecodeTimeInBucket(values[i])
		if err != nil {
			log.Error("analyze values from buckets", zap.String("column", col.Name.O),
				dmlog.ZapRedactString("value", values[i]), logutil.ZapRedactError(err))
			return nil, errors.Trace(err)
		}
		values[i] = value
	}

	return values, nil
}
