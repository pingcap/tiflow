package entry

import (
	"fmt"

	. "github.com/pingcap/check"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
)

type schemaBuilderSuite struct{}

var _ = Suite(&schemaBuilderSuite{})

func (s *schemaBuilderSuite) TestStorageBuilder(c *C) {
	c.Skip("not implemented")
	historyJobs := []*timodel.Job{{
		ID:       1,
		State:    timodel.JobStateSynced,
		SchemaID: 1,
		Type:     timodel.ActionCreateSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 1,
			DBInfo: &timodel.DBInfo{
				ID:    1,
				Name:  timodel.NewCIStr("testDB"),
				State: timodel.StatePublic,
			}, FinishedTS: 10},
		Query: "create database testDB",
	},
		buildCreateTableJob(2, 1, 1, "testTBL1", 12),
		buildCreateTableJob(3, 1, 2, "testTBL2", 14),
	}
	ddlEventCh := make(chan *model.RawKVEntry)
	go func() {
		for i := 4; i < 10; i++ {
			job := buildCreateTableJob(int64(i), 1, int64(i-1), fmt.Sprintf("testTBL%d", i-1), uint64(i*10))
			ddlEventCh <- job2RawKvEntry(job)
		}
	}()
	b, err := NewStorageBuilder(historyJobs, ddlEventCh)
	c.Assert(err, IsNil)
	b.Build(0)
}

func buildCreateTableJob(jobID int64, schemaID int64, tableID int64, tableName string, finishedTs uint64) *timodel.Job {
	return &timodel.Job{
		ID:       jobID,
		State:    timodel.JobStateSynced,
		SchemaID: schemaID,
		TableID:  tableID,
		Type:     timodel.ActionCreateTable,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 2,
			TableInfo: &timodel.TableInfo{
				ID:    tableID,
				Name:  timodel.NewCIStr(tableName),
				State: timodel.StatePublic,
			}, FinishedTS: finishedTs},
		Query: "create table " + tableName,
	}
}

func job2RawKvEntry(job *timodel.Job) *model.RawKVEntry {
	return nil
}
