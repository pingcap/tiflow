package model

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/cyclic"
)

type cyclicSuit struct{}

var _ = check.Suite(&cyclicSuit{})

func TestCyclic(t *testing.T) { check.TestingT(t) }

func (s *cyclicSuit) TestMapMarkRowsGroupReduceCyclicRowsGroup(c *check.C) {
	rID := cyclic.CyclicReplicaIDCol
	tests := []struct {
		input    map[TableName][][]*RowChangedEvent
		output   map[uint64]map[TableName][]*RowChangedEvent
		markMap  map[uint64]*RowChangedEvent
		reduced  map[TableName][][]*RowChangedEvent
		filterID []uint64
	}{
		{
			input:    map[TableName][][]*RowChangedEvent{},
			output:   map[uint64]map[TableName][]*RowChangedEvent{},
			markMap:  map[uint64]*RowChangedEvent{},
			reduced:  map[TableName][][]*RowChangedEvent{},
			filterID: []uint64{},
		},
		{
			input:    map[TableName][][]*RowChangedEvent{{Table: "a"}: {{{StartTs: 1}}}},
			output:   map[uint64]map[TableName][]*RowChangedEvent{1: {{Table: "a"}: {{StartTs: 1}}}},
			markMap:  map[uint64]*RowChangedEvent{},
			reduced:  map[TableName][][]*RowChangedEvent{{Table: "a"}: {{{StartTs: 1}}}},
			filterID: []uint64{},
		},
		{
			input: map[TableName][][]*RowChangedEvent{
				{Schema: "tidb_cdc"} /* cyclic.SchemaName */ : {{
					{StartTs: 1, Columns: map[string]*Column{rID: {Value: uint64(10)}}},
				}},
			},
			output: map[uint64]map[TableName][]*RowChangedEvent{},
			markMap: map[uint64]*RowChangedEvent{
				1: {StartTs: 1, Columns: map[string]*Column{rID: {Value: uint64(10)}}},
			},
			reduced:  map[TableName][][]*RowChangedEvent{},
			filterID: []uint64{},
		},
		{
			input: map[TableName][][]*RowChangedEvent{
				{Table: "a"}:         {{{StartTs: 1}}},
				{Schema: "tidb_cdc"}: {{{StartTs: 1, Columns: map[string]*Column{rID: {Value: uint64(10)}}}}},
			},
			output: map[uint64]map[TableName][]*RowChangedEvent{1: {{Table: "a"}: {{StartTs: 1}}}},
			markMap: map[uint64]*RowChangedEvent{
				1: {StartTs: 1, Columns: map[string]*Column{rID: {Value: uint64(10)}}},
			},
			reduced:  map[TableName][][]*RowChangedEvent{},
			filterID: []uint64{10},
		},
		{
			input: map[TableName][][]*RowChangedEvent{
				{Table: "a"}:                     {{{StartTs: 1}}},
				{Schema: "tidb_cdc", Table: "1"}: {{{StartTs: 1, Columns: map[string]*Column{rID: {Value: uint64(10)}}}}},
				{Schema: "tidb_cdc", Table: "2"}: {{{StartTs: 2, Columns: map[string]*Column{rID: {Value: uint64(10)}}}}},
				{Schema: "tidb_cdc", Table: "3"}: {{{StartTs: 3, Columns: map[string]*Column{rID: {Value: uint64(10)}}}}},
			},
			output: map[uint64]map[TableName][]*RowChangedEvent{1: {{Table: "a"}: {{StartTs: 1}}}},
			markMap: map[uint64]*RowChangedEvent{
				1: {StartTs: 1, Columns: map[string]*Column{rID: {Value: uint64(10)}}},
				2: {StartTs: 2, Columns: map[string]*Column{rID: {Value: uint64(10)}}},
				3: {StartTs: 3, Columns: map[string]*Column{rID: {Value: uint64(10)}}},
			},
			reduced:  map[TableName][][]*RowChangedEvent{},
			filterID: []uint64{10},
		},
		{
			input: map[TableName][][]*RowChangedEvent{
				{Table: "a"}:                     {{{StartTs: 1}}},
				{Table: "b2"}:                    {{{StartTs: 2}}},
				{Table: "b2.1"}:                  {{{StartTs: 2}}},
				{Schema: "tidb_cdc", Table: "1"}: {{{StartTs: 1, Columns: map[string]*Column{rID: {Value: uint64(10)}}}}},
			},
			output: map[uint64]map[TableName][]*RowChangedEvent{
				1: {{Table: "a"}: {{StartTs: 1}}},
				2: {{Table: "b2"}: {{StartTs: 2}}, {Table: "b2.1"}: {{StartTs: 2}}},
			},
			markMap: map[uint64]*RowChangedEvent{
				1: {StartTs: 1, Columns: map[string]*Column{rID: {Value: uint64(10)}}},
			},
			reduced:  map[TableName][][]*RowChangedEvent{{Table: "b2"}: {{{StartTs: 2}}}, {Table: "b2.1"}: {{{StartTs: 2}}}},
			filterID: []uint64{10},
		},
		{
			input: map[TableName][][]*RowChangedEvent{
				{Table: "a"}:    {{{StartTs: 1}}},
				{Table: "b2"}:   {{{StartTs: 2}}},
				{Table: "b2.1"}: {{{StartTs: 2}}},
				{Table: "b3"}:   {{{StartTs: 3, Table: &TableName{}}}},
				{Table: "b3.1"}: {{{StartTs: 3, Table: &TableName{}}}},
				{Schema: "tidb_cdc", Table: "1"}: {{
					{StartTs: 2, Columns: map[string]*Column{rID: {Value: uint64(10)}}},
					{StartTs: 3, Columns: map[string]*Column{rID: {Value: uint64(11)}}},
				}},
			},
			output: map[uint64]map[TableName][]*RowChangedEvent{
				1: {{Table: "a"}: {{StartTs: 1}}},
				2: {{Table: "b2"}: {{StartTs: 2}}, {Table: "b2.1"}: {{StartTs: 2}}},
				3: {{Table: "b3"}: {{StartTs: 3, Table: &TableName{}}},
					{Table: "b3.1"}: {{StartTs: 3, Table: &TableName{}}}},
			},
			markMap: map[uint64]*RowChangedEvent{
				2: {StartTs: 2, Columns: map[string]*Column{rID: {Value: uint64(10)}}},
				3: {StartTs: 3, Columns: map[string]*Column{rID: {Value: uint64(11)}}},
			},
			reduced: map[TableName][][]*RowChangedEvent{
				{Table: "a"}: {{{StartTs: 1}}},
				{Table: "b3"}: {{
					{StartTs: 3, Columns: map[string]*Column{rID: {Value: uint64(11)}}, Table: &TableName{Schema: "tidb_cdc", Table: "repl_mark__b3"}},
					{StartTs: 3, Table: &TableName{}},
				}},
				{Table: "b3.1"}: {{
					{StartTs: 3, Columns: map[string]*Column{rID: {Value: uint64(11)}}, Table: &TableName{Schema: "tidb_cdc", Table: "repl_mark__b3_1"}},
					{StartTs: 3, Table: &TableName{}},
				}},
			},
			filterID: []uint64{10}, // 10 -> 2, filter start ts 2
		},
		{
			input: map[TableName][][]*RowChangedEvent{
				{Table: "b2"}: {{{StartTs: 2, CRTs: 2}}},
				{Table: "b3"}: {{
					{StartTs: 2, CRTs: 2, Table: &TableName{}},
					{StartTs: 3, CRTs: 3, Table: &TableName{}},
					{StartTs: 3, CRTs: 3, Table: &TableName{}},
					{StartTs: 4, CRTs: 4, Table: &TableName{}},
				}},
				{Schema: "tidb_cdc", Table: "1"}: {{
					{StartTs: 2, CRTs: 2, Columns: map[string]*Column{rID: {Value: uint64(10)}}},
					{StartTs: 3, CRTs: 3, Columns: map[string]*Column{rID: {Value: uint64(11)}}},
				}},
			},
			output: map[uint64]map[TableName][]*RowChangedEvent{
				2: {{Table: "b2"}: {{StartTs: 2, CRTs: 2}},
					{Table: "b3"}: {{StartTs: 2, CRTs: 2, Table: &TableName{}}}},
				3: {{Table: "b3"}: {{StartTs: 3, CRTs: 3, Table: &TableName{}},
					{StartTs: 3, CRTs: 3, Table: &TableName{}}}},
				4: {{Table: "b3"}: {{StartTs: 4, CRTs: 4, Table: &TableName{}}}},
			},
			markMap: map[uint64]*RowChangedEvent{
				2: {StartTs: 2, CRTs: 2, Columns: map[string]*Column{rID: {Value: uint64(10)}}},
				3: {StartTs: 3, CRTs: 3, Columns: map[string]*Column{rID: {Value: uint64(11)}}},
			},
			reduced: map[TableName][][]*RowChangedEvent{
				{Table: "b3"}: {
					{{StartTs: 3, CRTs: 3, Columns: map[string]*Column{rID: {Value: uint64(11)}}, Table: &TableName{Schema: "tidb_cdc", Table: "repl_mark__b3"}},
						{StartTs: 3, CRTs: 3, Table: &TableName{}},
						{StartTs: 3, CRTs: 3, Table: &TableName{}}},
					{{StartTs: 4, CRTs: 4, Table: &TableName{}}},
				},
			},
			filterID: []uint64{10}, // 10 -> 2, filter start ts 2
		},
	}

	prettyPrint := func(v interface{}) string {
		return spew.Sprintf("%v", v)
	}

	for i, test := range tests {
		output, markMap := MapMarkRowsGroup(test.input)
		checkOutput := func(m1, m2 map[uint64]map[TableName][]*RowChangedEvent) {
			c.Assert(len(m1), check.DeepEquals, len(m2),
				check.Commentf("case %d %+v\n%+v\n%+v", i, test, m1, m2))
			for k, v := range m1 {
				c.Assert(v, check.DeepEquals, m2[k],
					check.Commentf("case %d %+v\n%+v\n%+v", i, test, m1, m2))
			}
		}
		checkOutput(output, test.output)

		checkMarkmap := func(m1, m2 map[uint64]*RowChangedEvent) {
			c.Assert(len(m1), check.DeepEquals, len(m2),
				check.Commentf("case %d %+v\n%+v\n%+v", i, test, m1, m2))
			for k, v := range m1 {
				c.Assert(v, check.DeepEquals, m2[k],
					check.Commentf("case %d %+v\n%+v\n%+v", i, test, m1, m2))
			}
		}
		checkMarkmap(markMap, test.markMap)

		checkReduce := func(m1, m2 map[TableName][][]*RowChangedEvent) {
			c.Assert(len(m1), check.DeepEquals, len(m2),
				check.Commentf("case %d %+v\n%s\n%s", i, test, prettyPrint(m1), prettyPrint(m2)))
			for k, v := range m1 {
				c.Assert(v, check.DeepEquals, m2[k],
					check.Commentf("case %d %+v\n%s\n%s", i, test, prettyPrint(m1), prettyPrint(m2)))
			}
		}
		reduced := ReduceCyclicRowsGroup(output, markMap, test.filterID)
		checkReduce(reduced, test.reduced)
	}
}
