package sink

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama/mocks"
	"github.com/pingcap/check"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
)

type KafkaEmitSuite struct{}

var _ = check.Suite(&KafkaEmitSuite{})

type reporter struct {

}

func (r reporter) Errorf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

func (s KafkaEmitSuite) TestKafkaShouldExecDDL(c *check.C) {
	// Set up
	p := mocks.NewSyncProducer(reporter{}, nil)
	p.ExpectSendMessageAndSucceed()
	defer  p.Close()
	helper := tableHelper{}
	sink, err := NewKafkaSink("cdc1", "abc",  1, p, &helper)
	c.Assert(err, check.IsNil)

	t := model.Txn{
		DDL: &model.DDL{
			Database: "test",
			Table:    "user",
			Job: &timodel.Job{
				Query: "CREATE TABLE user (id INT PRIMARY KEY);",
			},
		},
	}
	// Execute
	err = sink.Emit(context.Background(), t)

	// Validate
	c.Assert(err, check.IsNil)
}
