package filter

import (
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestShouldIgnoreReplicationEvent(t *testing.T) {
	cases := []struct {
		cfg        *config.FilterConfig
		innerCases []struct {
			schema string
			table  string
			ignore bool
		}
	}{
		{
			// all tables event will be ignored, since match is "*.*" and
			// IgnoreRowsWrittenByTiCDC is true
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:                  []string{"*.*"},
						IgnoreRowsWrittenByTiCDC: true,
					},
				},
			},
			innerCases: []struct {
				schema string
				table  string
				ignore bool
			}{
				{
					schema: "test",
					table:  "t1",
					ignore: true,
				},
				{
					schema: "test2",
					table:  "t2",
					ignore: true,
				},
			},
		},

		{
			// all tables event will be ignored, since match is "*.*" and
			// IgnoreRowsWrittenByTiCDC is true
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:                  []string{"test.t1"},
						IgnoreRowsWrittenByTiCDC: true,
					},
					{
						Matcher:                  []string{"test2.t2"},
						IgnoreRowsWrittenByTiCDC: true,
					},
					{ // event of "test3.t3" will not be ignored, since IgnoreRowsWrittenByTiCDC is false
						Matcher:                  []string{"test3.t3"},
						IgnoreRowsWrittenByTiCDC: false,
					},
				},
			},
			innerCases: []struct {
				schema string
				table  string
				ignore bool
			}{
				{
					schema: "test",
					table:  "t1",
					ignore: true,
				},
				{
					schema: "test2",
					table:  "t2",
					ignore: true,
				},
				{
					schema: "test3",
					table:  "t3",
					ignore: false,
				},
			},
		},
	}

	for _, c := range cases {
		rf, err := newReplicationFilter(c.cfg)
		require.NoErrorf(t, err, "new replication filter failed, cfg: %v", c.cfg)
		for _, inerCase := range c.innerCases {
			ignore := rf.shouldIgnoreReplicationEvent(inerCase.schema, inerCase.table)
			if ignore != inerCase.ignore {
				require.Equal(t, inerCase.ignore, ignore, "schema: %s, table: %s", inerCase.schema, inerCase.table)
			}
		}
	}

}
