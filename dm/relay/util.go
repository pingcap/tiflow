// Copyright 2019 PingCAP, Inc.
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

package relay

import (
	"context"
	"io"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

// isNewServer checks whether is connecting to a new server.
func isNewServer(ctx context.Context, prevUUID string, db *conn.BaseDB, flavor string) (bool, error) {
	if len(prevUUID) == 0 {
		// no sub dir exists before
		return true, nil
	}
	uuid, err := conn.GetServerUUID(tcontext.NewContext(ctx, log.L()), db, flavor)
	if err != nil {
		return false, err
	}
	if strings.HasPrefix(prevUUID, uuid) {
		// same server as before
		return false, nil
	}
	return true, nil
}

// getNextRelaySubDir gets next relay log subdirectory after the current subdirectory.
func getNextRelaySubDir(currSubDir string, subDirs []string) (string, string, error) {
	for i := len(subDirs) - 2; i >= 0; i-- {
		if subDirs[i] == currSubDir {
			nextSubDir := subDirs[i+1]
			_, suffixInt, err := utils.ParseRelaySubDir(nextSubDir)
			if err != nil {
				return "", "", terror.Annotatef(err, "UUID %s", nextSubDir)
			}
			return nextSubDir, utils.SuffixIntToStr(suffixInt), nil
		}
	}
	return "", "", nil
}

// isIgnorableParseError checks whether is a ignorable error for `BinlogParser.ParseFile`.
func isIgnorableParseError(err error) bool {
	if err == nil {
		return false
	}

	if strings.Contains(err.Error(), "err EOF") {
		// NOTE: go-mysql returned err not includes caused err, but as message, ref: parser.go `parseSingleEvent`
		return true
	} else if errors.Cause(err) == io.EOF {
		return true
	}

	return false
}
