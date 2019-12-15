package sink

import (
	"fmt"
	"github.com/pingcap/check"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
	dbtypes "github.com/pingcap/tidb/types"
)

type EncodeSuite struct{}

var _ = check.Suite(&EncodeSuite{})

func (s EncodeSuite) TestShouldDecodeResolve(c *check.C) {
	data, err := NewResloveTsWriter("abc", 1).Write()
	c.Assert(err, check.IsNil)

	r := NewReader(data)
	m, err := r.Decode()
	c.Assert(err, check.IsNil)

	fmt.Println(m.CdcID)
	fmt.Println(m.ResloveTs)
}

func (s EncodeSuite) TestShouldDecodeDDLTxn(c *check.C) {
	t := model.Txn{
		DDL: &model.DDL{
			Database: "test",
			Table:    "user",
			Job: &timodel.Job{
				Query: "CREATE TABLE user (id INT PRIMARY KEY);",
			},
		},
	}
	helper := tableHelper{}

	data, err := NewTxnWriter("abc", t, helper.TableInfoGetter).Write()
	c.Assert(err, check.IsNil)

	r := NewReader(data)
	m, err := r.Decode()
	c.Assert(err, check.IsNil)

	fmt.Println(m.CdcID)
	fmt.Println("table=" + m.Txn.DDL.Table)
}

func (s EncodeSuite) TestShouldDecodeDMLTxn(c *check.C) {
	t := model.Txn{
		DMLs: []*model.DML{
			{
				Database: "test",
				Table:    "user",
				Tp:       model.InsertDMLType,
				Values: map[string]dbtypes.Datum{
					"id":   dbtypes.NewDatum(42),
					"name": dbtypes.NewDatum("tester1"),
				},
			},
		},
	}
	helper := &tableHelper{}

	data, err := NewTxnWriter("abc", t, helper).Write()
	c.Assert(err, check.IsNil)

	r := NewReader(data)
	m, err := r.Decode()
	c.Assert(err, check.IsNil)

	fmt.Println(m.CdcID)
	for _, dml := range m.Txn.DMLs {
		for key, v := range dml.Values {
			fmt.Println(fmt.Sprintf("key=%s, value=%v", key, v.GetValue()))
		}
	}
}

func (s EncodeSuite) TestDecodeMeta(c *check.C) {
	cdcList := []string{"a", "b"}
	data, err := NewMetaWriter(cdcList, 5).Write()
	c.Assert(err, check.IsNil)

	m, err := NewReader(data).Decode()
	c.Assert(err, check.IsNil)

	fmt.Println(fmt.Sprintf("count=%d", m.MetaCount))
	for _, cdc := range m.CdcList {
		fmt.Println("cdc:" + cdc)
	}
}
