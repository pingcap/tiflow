package kv

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
)

type matcher struct {
	// TODO : clear the single prewrite
	unmatchedValue map[matchKey][]byte
}

type matchKey struct {
	startTs uint64
	key     string
}

func newMatchKey(row *cdcpb.Event_Row) matchKey {
	return matchKey{startTs: row.GetStartTs(), key: string(row.GetKey())}
}

func newMatcher() *matcher {
	return &matcher{
		unmatchedValue: make(map[matchKey][]byte),
	}
}

func (m *matcher) putPrewriteRow(row *cdcpb.Event_Row) {
	m.unmatchedValue[newMatchKey(row)] = row.GetValue()
}

func (m *matcher) matchRow(row *cdcpb.Event_Row) ([]byte, error) {
	if value, exist := m.unmatchedValue[newMatchKey(row)]; exist {
		delete(m.unmatchedValue, newMatchKey(row))
		return value, nil
	}
	return nil, errors.NotFoundf("prewrite row, startTs:%d", row.GetStartTs())

}

func (m *matcher) rollbackRow(row *cdcpb.Event_Row) {
	delete(m.unmatchedValue, newMatchKey(row))
}
