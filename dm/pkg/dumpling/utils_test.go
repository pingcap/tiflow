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

package dumpling

import (
	"context"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestParseMetaData(t *testing.T) {
	t.Parallel()
	f, err := os.CreateTemp("", "metadata")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	fdir := path.Dir(f.Name())
	fname := path.Base(f.Name())

	testCases := []struct {
		source   string
		pos      mysql.Position
		gsetStr  string
		loc2     bool
		pos2     mysql.Position
		gsetStr2 string
	}{
		{
			`Started dump at: 2018-12-28 07:20:49
SHOW MASTER STATUS:
        Log: bin.000001
        Pos: 2479
        GTID:97b5142f-e19c-11e8-808c-0242ac110005:1-13

Finished dump at: 2018-12-28 07:20:51`,
			mysql.Position{
				Name: "bin.000001",
				Pos:  2479,
			},
			"97b5142f-e19c-11e8-808c-0242ac110005:1-13",
			false,
			mysql.Position{},
			"",
		},
		{
			`Started dump at: 2018-12-27 19:51:22
SHOW MASTER STATUS:
        Log: mysql-bin.000003
        Pos: 3295817
        GTID:

SHOW SLAVE STATUS:
        Host: 10.128.27.98
        Log: mysql-bin.000003
        Pos: 329635
        GTID:

Finished dump at: 2018-12-27 19:51:22`,
			mysql.Position{
				Name: "mysql-bin.000003",
				Pos:  3295817,
			},
			"",
			false,
			mysql.Position{},
			"",
		},
		{ // with empty line after multiple GTID sets
			`Started dump at: 2020-05-21 18:14:49
SHOW MASTER STATUS:
	Log: mysql-bin.000003
	Pos: 1274
	GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-7,
97b5142f-e19c-11e8-808c-0242ac110005:1-13

SHOW SLAVE STATUS:
	Host: 192.168.100.100
	Log: mysql-bin.000003
	Pos: 700
	GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-7,
97b5142f-e19c-11e8-808c-0242ac110005:1-13

Finished dump at: 2020-05-21 18:14:49`,
			mysql.Position{
				Name: "mysql-bin.000003",
				Pos:  1274,
			},
			"5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,5b642cb6-9b43-11ea-8914-0242ac170003:1-7,97b5142f-e19c-11e8-808c-0242ac110005:1-13",
			false,
			mysql.Position{},
			"",
		},
		{ // without empty line after mutlple GTID sets
			`Started dump at: 2020-05-21 18:02:33
SHOW MASTER STATUS:
		Log: mysql-bin.000003
		Pos: 1274
		 GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-7,
97b5142f-e19c-11e8-808c-0242ac110005:1-13
Finished dump at: 2020-05-21 18:02:44`,
			mysql.Position{
				Name: "mysql-bin.000003",
				Pos:  1274,
			},
			"5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,5b642cb6-9b43-11ea-8914-0242ac170003:1-7,97b5142f-e19c-11e8-808c-0242ac110005:1-13",
			false,
			mysql.Position{},
			"",
		},
		{ // with empty line after multiple GTID sets
			`Started dump at: 2020-05-21 18:14:49
SHOW MASTER STATUS:
	Log: mysql-bin.000003
	Pos: 1274
	GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-7,
97b5142f-e19c-11e8-808c-0242ac110005:1-13

SHOW SLAVE STATUS:
	Host: 192.168.100.100
	Log: mysql-bin.000003
	Pos: 700
	GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-7,
97b5142f-e19c-11e8-808c-0242ac110005:1-13

SHOW MASTER STATUS: /* AFTER CONNECTION POOL ESTABLISHED */
	Log: mysql-bin.000003
	Pos: 1280
	GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-7,
97b5142f-e19c-11e8-808c-0242ac110005:1-14

Finished dump at: 2020-05-21 18:14:49`,
			mysql.Position{
				Name: "mysql-bin.000003",
				Pos:  1274,
			},
			"5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,5b642cb6-9b43-11ea-8914-0242ac170003:1-7,97b5142f-e19c-11e8-808c-0242ac110005:1-13",
			true,
			mysql.Position{
				Name: "mysql-bin.000003",
				Pos:  1280,
			},
			"5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,5b642cb6-9b43-11ea-8914-0242ac170003:1-7,97b5142f-e19c-11e8-808c-0242ac110005:1-14",
		},
		{ // with empty line after multiple GTID sets
			`Started dump at: 2020-05-21 18:14:49
SHOW MASTER STATUS:
	Log: mysql-bin.000003
	Pos: 1274
	GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-7,
97b5142f-e19c-11e8-808c-0242ac110005:1-13

SHOW SLAVE STATUS:
	Host: 192.168.100.100
	Log: mysql-bin.000003
	Pos: 700
	GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-7,
97b5142f-e19c-11e8-808c-0242ac110005:1-13

SHOW MASTER STATUS: /* AFTER CONNECTION POOL ESTABLISHED */
	Log: mysql-bin.000004
	Pos: 4
	GTID:5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,
5b642cb6-9b43-11ea-8914-0242ac170003:1-9,
97b5142f-e19c-11e8-808c-0242ac110005:1-13

Finished dump at: 2020-05-21 18:14:49`,
			mysql.Position{
				Name: "mysql-bin.000003",
				Pos:  1274,
			},
			"5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,5b642cb6-9b43-11ea-8914-0242ac170003:1-7,97b5142f-e19c-11e8-808c-0242ac110005:1-13",
			true,
			mysql.Position{
				Name: "mysql-bin.000004",
				Pos:  4,
			},
			"5b5a8e4e-9b43-11ea-900d-0242ac170002:1-10,5b642cb6-9b43-11ea-8914-0242ac170003:1-9,97b5142f-e19c-11e8-808c-0242ac110005:1-13",
		},
		{ // no GTID sets
			`Started dump at: 2020-09-30 12:16:49
SHOW MASTER STATUS:
	Log: mysql-bin-changelog.000003
	Pos: 12470000

SHOW MASTER STATUS: /* AFTER CONNECTION POOL ESTABLISHED */
	Log: mysql-bin-changelog.000003
	Pos: 12470000

Finished dump at: 2020-09-30 12:16:49
`,
			mysql.Position{
				Name: "mysql-bin-changelog.000003",
				Pos:  12470000,
			},
			"",
			true,
			mysql.Position{
				Name: "mysql-bin-changelog.000003",
				Pos:  12470000,
			},
			"",
		},
	}
	ctx := context.Background()
	for _, tc := range testCases {
		err2 := os.WriteFile(f.Name(), []byte(tc.source), 0o644)
		require.NoError(t, err2)
		loc, loc2, err2 := ParseMetaData(ctx, fdir, fname, "mysql", nil)
		require.NoError(t, err2)
		require.Equal(t, tc.pos, loc.Position)
		gs, _ := gtid.ParserGTID("mysql", tc.gsetStr)
		require.Equal(t, gs, loc.GetGTID())
		if tc.loc2 {
			require.Equal(t, tc.pos2, loc2.Position)
			gs2, _ := gtid.ParserGTID("mysql", tc.gsetStr2)
			require.Equal(t, gs2, loc2.GetGTID())
		} else {
			require.Nil(t, loc2)
		}
	}

	noBinlogLoc := `Started dump at: 2020-12-02 17:13:56
Finished dump at: 2020-12-02 17:13:56
`
	err = os.WriteFile(f.Name(), []byte(noBinlogLoc), 0o644)
	require.NoError(t, err)
	_, _, err = ParseMetaData(ctx, fdir, fname, "mysql", nil)
	require.True(t, terror.ErrMetadataNoBinlogLoc.Equal(err))
}

func TestParseArgs(t *testing.T) {
	t.Parallel()
	logger := log.L()

	exportCfg := export.DefaultConfig()
	extraArgs := `--statement-size=100 --where t>10 --threads 8 -F 50B`
	err := ParseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	require.NoError(t, err)
	require.Equal(t, uint64(100), exportCfg.StatementSize)
	require.Equal(t, "t>10", exportCfg.Where)
	require.Equal(t, 8, exportCfg.Threads)
	require.Equal(t, uint64(50), exportCfg.FileSize)

	extraArgs = `--threads 16 --skip-tz-utc`
	err = ParseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	require.Error(t, err)
	require.Equal(t, 16, exportCfg.Threads)
	require.Equal(t, uint64(100), exportCfg.StatementSize)

	// no `--tables-list` or `--filter` specified, match anything
	require.True(t, exportCfg.TableFilter.MatchTable("foo", "bar"))
	require.True(t, exportCfg.TableFilter.MatchTable("bar", "foo"))

	// specify `--tables-list`.
	extraArgs = `--threads 16 --tables-list=foo.bar`
	err = ParseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	require.NoError(t, err)
	require.True(t, exportCfg.TableFilter.MatchTable("foo", "bar"))
	require.False(t, exportCfg.TableFilter.MatchTable("bar", "foo"))

	// specify `--tables-list` and `--filter`
	extraArgs = `--threads 16 --tables-list=foo.bar --filter=*.foo`
	err = ParseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	require.Regexp(t, ".*--tables-list and --filter together.*", err)

	// only specify `--filter`.
	extraArgs = `--threads 16 --filter=*.foo`
	err = ParseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	require.NoError(t, err)
	require.False(t, exportCfg.TableFilter.MatchTable("foo", "bar"))
	require.True(t, exportCfg.TableFilter.MatchTable("bar", "foo"))

	// compatibility for `--no-locks`
	extraArgs = `--no-locks`
	err = ParseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	require.NoError(t, err)
	require.Equal(t, "none", exportCfg.Consistency)

	// compatibility for `--no-locks`
	extraArgs = `--no-locks --consistency none`
	err = ParseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	require.NoError(t, err)
	require.Equal(t, "none", exportCfg.Consistency)

	extraArgs = `--consistency lock`
	err = ParseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	require.NoError(t, err)
	require.Equal(t, "lock", exportCfg.Consistency)

	// compatibility for `--no-locks`
	extraArgs = `--no-locks --consistency lock`
	err = ParseExtraArgs(&logger, exportCfg, strings.Fields(extraArgs))
	require.Equal(t, "cannot both specify `--no-locks` and `--consistency` other than `none`", err.Error())
}
