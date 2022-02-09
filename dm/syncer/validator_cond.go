package syncer

import (
	"strings"

	"github.com/pingcap/tidb-tools/pkg/dbutil"
)

type Cond struct {
	Table    *TableDiff
	PkValues [][]string
}

func (c *Cond) GetArgs() []interface{} {
	var res []interface{}
	for _, v := range c.PkValues {
		for _, val := range v {
			res = append(res, val)
		}
	}
	return res
}

func (c *Cond) GetWhere() string {
	var b strings.Builder
	pk := c.Table.PrimaryKey
	if len(pk.Columns) > 1 {
		// TODO
		panic("should be one")
	}
	b.WriteString(pk.Columns[0].Name.O)
	b.WriteString(" in (")
	for i := range c.PkValues {
		if i != 0 {
			b.WriteString(", ")
		}
		b.WriteString("?")
	}
	b.WriteString(")")
	return b.String()
}

type SimpleRowsIterator struct {
	Rows []map[string]*dbutil.ColumnData
	Idx  int
}

func (b *SimpleRowsIterator) Next() (map[string]*dbutil.ColumnData, error) {
	if b.Idx >= len(b.Rows) {
		return nil, nil
	}
	row := b.Rows[b.Idx]
	b.Idx++
	return row, nil
}

func (b *SimpleRowsIterator) Close() {
	// skip
}
