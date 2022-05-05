//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package common

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseLogFileName(t *testing.T) {
	type arg struct {
		name string
	}
	// the log looks like: fmt.Sprintf("%s_%s_%d_%s_%d%s", w.cfg.captureID, w.cfg.changeFeedID, w.cfg.createTime.Unix(), w.cfg.fileType, w.commitTS.Load(), redo.LogEXT)
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
				name: fmt.Sprintf("%s_%s_%d_%s_%d%s", "cp",
					"test",
					time.Now().Unix(), DefaultRowLogFileType, 1, LogEXT),
			},
			wantTs:       1,
			wantFileType: DefaultRowLogFileType,
		},
		{
			name: "happy row .log",
			args: arg{
				name: fmt.Sprintf("%s_%s_%s_%d_%s_%d%s", "cp",
					"namespace", "test",
					time.Now().Unix(), DefaultRowLogFileType, 1, LogEXT),
			},
			wantTs:       1,
			wantFileType: DefaultRowLogFileType,
		},
		{
			name: "happy row .tmp",
			args: arg{
				name: fmt.Sprintf("%s_%s_%d_%s_%d%s", "cp",
					"test",
					time.Now().Unix(), DefaultRowLogFileType, 1, LogEXT) + TmpEXT,
			},
			wantTs:       1,
			wantFileType: DefaultRowLogFileType,
		},
		{
			name: "happy row .tmp",
			args: arg{
				name: fmt.Sprintf("%s_%s_%s_%d_%s_%d%s", "cp",
					"namespace", "test",
					time.Now().Unix(), DefaultRowLogFileType, 1, LogEXT) + TmpEXT,
			},
			wantTs:       1,
			wantFileType: DefaultRowLogFileType,
		},
		{
			name: "happy ddl .log",
			args: arg{
				name: fmt.Sprintf("%s_%s_%d_%s_%d%s", "cp",
					"test",
					time.Now().Unix(), DefaultDDLLogFileType, 1, LogEXT),
			},
			wantTs:       1,
			wantFileType: DefaultDDLLogFileType,
		},
		{
			name: "happy ddl .log",
			args: arg{
				name: fmt.Sprintf("%s_%s_%s_%d_%s_%d%s", "cp",
					"namespace", "test",
					time.Now().Unix(), DefaultDDLLogFileType, 1, LogEXT),
			},
			wantTs:       1,
			wantFileType: DefaultDDLLogFileType,
		},
		{
			name: "happy ddl .sort",
			args: arg{
				name: fmt.Sprintf("%s_%s_%s_%d_%s_%d%s", "cp",
					"default", "test",
					time.Now().Unix(), DefaultDDLLogFileType, 1, LogEXT) + SortLogEXT,
			},
			wantTs:       1,
			wantFileType: DefaultDDLLogFileType,
		},
		{
			name: "happy ddl .sort",
			args: arg{
				name: fmt.Sprintf("%s_%s_%s_%d_%s_%d%s", "cp",
					"namespace", "test",
					time.Now().Unix(), DefaultDDLLogFileType, 1, LogEXT) + SortLogEXT,
			},
			wantTs:       1,
			wantFileType: DefaultDDLLogFileType,
		},
		{
			name: "happy ddl .tmp",
			args: arg{
				name: fmt.Sprintf("%s_%s_%d_%s_%d%s", "cp",
					"test",
					time.Now().Unix(), DefaultDDLLogFileType, 1, LogEXT) + TmpEXT,
			},
			wantTs:       1,
			wantFileType: DefaultDDLLogFileType,
		},
		{
			name: "happy ddl .tmp",
			args: arg{
				name: fmt.Sprintf("%s_%s_%s_%d_%s_%d%s", "cp",
					"namespace", "test",
					time.Now().Unix(), DefaultDDLLogFileType, 1, LogEXT) + TmpEXT,
			},
			wantTs:       1,
			wantFileType: DefaultDDLLogFileType,
		},
		{
			name: "happy .meta",
			args: arg{
				name: "sdfsdfsf" + MetaEXT,
			},
			wantTs:       0,
			wantFileType: DefaultMetaFileType,
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
				name: fmt.Sprintf("%s_%s_%s_%d_%s%d%s", "cp",
					"default", "test",
					time.Now().Unix(), DefaultDDLLogFileType, 1, LogEXT) + TmpEXT,
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
