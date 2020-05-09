package notify

import (
	"context"
	"time"

	"github.com/pingcap/check"
)

type notifySuite struct{}

var _ = check.Suite(&notifySuite{})

func (s *notifySuite) TestNotifyHub(c *check.C) {
	ctx := context.Background()
	notifier := new(Notifier)
	r1 := notifier.NewReceiver(ctx, -1)
	r2 := notifier.NewReceiver(ctx, -1)
	r3 := notifier.NewReceiver(ctx, -1)
	go func() {
		for i := 0; i < 5; i++ {
			time.Sleep(time.Second)
			notifier.Notify(context.Background())
		}
	}()
	<-r1.C
	r1.Stop()
	<-r2.C
	<-r3.C

	r2.Stop()
	r3.Stop()
	c.Assert(len(notifier.notifyChs), check.Equals, 0)
	time.Sleep(time.Second)
	r4 := notifier.NewReceiver(ctx, -1)
	<-r4.C
	r4.Stop()

	notifier2 := new(Notifier)
	r5 := notifier2.NewReceiver(ctx, 10*time.Millisecond)
	<-r5.C
	r5.Stop()
}
