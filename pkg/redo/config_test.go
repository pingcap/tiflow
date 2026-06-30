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

package redo

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestParseLogFileName(t *testing.T) {
	t.Parallel()

	type arg struct {
		name string
	}
	tests := []struct {
		name         string
		args         arg
		wantTs       uint64
		wantFileType string
		wantErr      string
	}{
		{
			name: "happy row .log",
			args: arg{
				name: fmt.Sprintf(RedoLogFileFormatV1, "cp",
					"test",
					RedoRowLogFileType, 1, uuid.NewString(), LogEXT),
			},
			wantTs:       1,
			wantFileType: RedoRowLogFileType,
		},
		{
			name: "happy row .log",
			args: arg{
				name: fmt.Sprintf(RedoLogFileFormatV2, "cp",
					"namespace", "test",
					RedoRowLogFileType, 1, uuid.NewString(), LogEXT),
			},
			wantTs:       1,
			wantFileType: RedoRowLogFileType,
		},
		{
			name: "happy row .tmp",
			args: arg{
				name: fmt.Sprintf(RedoLogFileFormatV1, "cp",
					"test",
					RedoRowLogFileType, 1, uuid.NewString(), LogEXT) + TmpEXT,
			},
			wantTs:       1,
			wantFileType: RedoRowLogFileType,
		},
		{
			name: "happy row .tmp",
			args: arg{
				name: fmt.Sprintf(RedoLogFileFormatV2, "cp",
					"namespace", "test",
					RedoRowLogFileType, 1, uuid.NewString(), LogEXT) + TmpEXT,
			},
			wantTs:       1,
			wantFileType: RedoRowLogFileType,
		},
		{
			name: "happy ddl .log",
			args: arg{
				name: fmt.Sprintf(RedoLogFileFormatV1, "cp",
					"test",
					RedoDDLLogFileType, 1, uuid.NewString(), LogEXT),
			},
			wantTs:       1,
			wantFileType: RedoDDLLogFileType,
		},
		{
			name: "happy ddl .log",
			args: arg{
				name: fmt.Sprintf(RedoLogFileFormatV2, "cp",
					"namespace", "test",
					RedoDDLLogFileType, 1, uuid.NewString(), LogEXT),
			},
			wantTs:       1,
			wantFileType: RedoDDLLogFileType,
		},
		{
			name: "happy ddl .sort",
			args: arg{
				name: fmt.Sprintf(RedoLogFileFormatV2, "cp",
					"default", "test",
					RedoDDLLogFileType, 1, uuid.NewString(), LogEXT) + SortLogEXT,
			},
			wantTs:       1,
			wantFileType: RedoDDLLogFileType,
		},
		{
			name: "happy ddl .sort",
			args: arg{
				name: fmt.Sprintf(RedoLogFileFormatV2, "cp",
					"namespace", "test",
					RedoDDLLogFileType, 1, uuid.NewString(), LogEXT) + SortLogEXT,
			},
			wantTs:       1,
			wantFileType: RedoDDLLogFileType,
		},
		{
			name: "happy ddl .tmp",
			args: arg{
				name: fmt.Sprintf(RedoLogFileFormatV1, "cp",
					"test",
					RedoDDLLogFileType, 1, uuid.NewString(), LogEXT) + TmpEXT,
			},
			wantTs:       1,
			wantFileType: RedoDDLLogFileType,
		},
		{
			name: "happy ddl .tmp",
			args: arg{
				name: fmt.Sprintf(RedoLogFileFormatV2, "cp",
					"namespace", "test",
					RedoDDLLogFileType, 1, uuid.NewString(), LogEXT) + TmpEXT,
			},
			wantTs:       1,
			wantFileType: RedoDDLLogFileType,
		},
		{
			name: "happy .meta",
			args: arg{
				name: "sdfsdfsf" + MetaEXT,
			},
			wantTs:       0,
			wantFileType: RedoMetaFileType,
		},
		{
			name: "not supported fileType",
			args: arg{
				name: "sdfsdfsf.sfsf",
			},
		},
		{
			name: "err wrong format ddl .tmp",
			args: arg{
				name: fmt.Sprintf("%s_%s_%s_%s_%d%s%s", /* a wrong format */
					"cp", "default", "test",
					RedoDDLLogFileType, 1, uuid.NewString(), LogEXT) + TmpEXT,
			},
			wantErr: ".*bad log name*.",
		},
	}
	for _, tt := range tests {
		ts, fileType, err := ParseLogFileName(tt.args.name)
		if tt.wantErr != "" {
			require.Regexp(t, tt.wantErr, err, tt.name)
		} else {
			require.Nil(t, err, tt.name)
			require.EqualValues(t, tt.wantTs, ts, tt.name)
			require.Equal(t, tt.wantFileType, fileType, tt.name)
		}
	}
}

func TestInitExternalStorage(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	urls := []string{
		fmt.Sprintf("file://%s/test", dir),
	}

	for _, urlStr := range urls {
		url, err := storage.ParseRawURL(urlStr)
		require.NoError(t, err)
		_, err = InitExternalStorage(context.Background(), *url)
		require.NoError(t, err)
	}
}
