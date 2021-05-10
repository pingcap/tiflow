package owner

import (
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"math"
	"math/rand"
	"testing"
)

func Test(t *testing.T) { check.TestingT(t) }

var _ = check.Suite(&barrierSuite{})

type barrierSuite struct {
}

func (s *barrierSuite) TestBarrier(c *check.C) {
	b:=newBarriers()
	b.Update(DDLJobBarrier,2)
	b.Update(SyncPointBarrier,3)
	b.Update(FinishBarrier,1)
	tp,ts:=b.Min()
	c.Assert(tp,check.Equals,FinishBarrier)
	c.Assert(ts,check.Equals,uint64(1))

	b.Update(FinishBarrier,4)
	tp,ts=b.Min()
	c.Assert(tp,check.Equals,DDLJobBarrier)
	c.Assert(ts,check.Equals,uint64(2))

	b.Remove(DDLJobBarrier)
	tp,ts=b.Min()
	c.Assert(tp,check.Equals,SyncPointBarrier)
	c.Assert(ts,check.Equals,uint64(3))

	b.Update(FinishBarrier,1)
	tp,ts=b.Min()
	c.Assert(tp,check.Equals,FinishBarrier)
	c.Assert(ts,check.Equals,uint64(1))

	b.Update(DDLJobBarrier,5)
	tp,ts=b.Min()
	c.Assert(tp,check.Equals,FinishBarrier)
	c.Assert(ts,check.Equals,uint64(1))
}

func (s *barrierSuite) TestBarrierRandom(c *check.C) {
	maxBarrierType := 50
	maxBarrierTs:=1000000
	b:=newBarriers()
	expectedBarriers:=make(map[barrierType]model.Ts)

	// set a barrier which can not be removed to avoid the barrier map is empty
	b.Update(maxBarrierType,model.Ts(maxBarrierTs))
	expectedBarriers[maxBarrierType]=model.Ts(maxBarrierTs)

	for i:=0;i<100000;i++{
		switch rand.Intn(2) {
		case 0:
			tp:=rand.Intn(maxBarrierType)
			ts:=model.Ts(rand.Intn(maxBarrierTs))
			b.Update(tp,ts)
			expectedBarriers[tp]=ts
		case 1:
			tp:=rand.Intn(maxBarrierType)
			b.Remove(tp)
			delete(expectedBarriers,tp)
		}
		expectedMinTs:=uint64(math.MaxUint64)
		for _,ts:=range expectedBarriers{
			if ts<expectedMinTs{
				expectedMinTs=ts
			}
		}
		tp,ts:=b.Min()
		c.Assert(ts,check.Equals,expectedMinTs)
		c.Assert(expectedBarriers[tp],check.Equals,expectedMinTs)
	}


}
