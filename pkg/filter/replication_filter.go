package filter

import (
	tfilter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

type replicationFilter struct {
	tableMatcher tfilter.Filter
}

func newReplicationFilter(cfg *config.FilterConfig) (*replicationFilter, error) {
	matcherMap := make(map[string]struct{})
	for _, rule := range cfg.EventFilters {
		if rule.IgnoreRowsWrittenByTiCDC {
			// If ignore IgnoreRowsWrittenByTiCDC is enabled,
			// add the table pattern to the matcher map.
			for _, matcher := range rule.Matcher {
				matcherMap[matcher] = struct{}{}
			}
		}
	}

	matchers := make([]string, 0, len(matcherMap))
	for matcher := range matcherMap {
		matchers = append(matchers, matcher)
	}

	tf, err := tfilter.Parse(matchers)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, matchers)
	}

	return &replicationFilter{
		tableMatcher: tf,
	}, nil
}

func (f *replicationFilter) shouldIgnoreReplicationEvent(schema, table string) bool {
	return f.tableMatcher.MatchTable(schema, table)
}
