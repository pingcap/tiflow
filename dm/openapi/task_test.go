package openapi

import (
	"testing"

	"github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/terror"
)

var _ = check.Suite(&taskSuite{})

type taskSuite struct{}

func TestTask(t *testing.T) {
	check.TestingT(t)
}

func (t *taskSuite) TestTaskAdjust(c *check.C) {
	meta := "test"
	// test no error
	task1 := &Task{MetaSchema: &meta, OnDuplicate: TaskOnDuplicateError}
	c.Assert(task1.Adjust(), check.IsNil)
	c.Assert(*task1.MetaSchema, check.Equals, meta)

	// test error
	task2 := &Task{}
	c.Assert(terror.ErrOpenAPICommonError.Equal(task2.Adjust()), check.IsTrue)

	// test default meta
	task3 := &Task{OnDuplicate: TaskOnDuplicateError}
	c.Assert(task3.Adjust(), check.IsNil)
	c.Assert(*task3.MetaSchema, check.Equals, defaultMetaSchema)
}
