package notify

import (
	"time"

	"github.com/pingcap/check"
)

type notifySuite struct{}

var _ = check.Suite(&notifySuite{})

func (s *notifySuite) TestNotifyHub(c *check.C) {
	notifier := new(Notifier)
	r1 := notifier.NewReceiver(-1)
	r2 := notifier.NewReceiver(-1)
	r3 := notifier.NewReceiver(-1)
	go func() {
		for i := 0; i < 5; i++ {
			time.Sleep(time.Second)
			notifier.Notify()
		}
	}()
	<-r1.C
	r1.Stop()
	<-r2.C
	<-r3.C

	r2.Stop()
	r3.Stop()
	c.Assert(len(notifier.receivers), check.Equals, 0)
	time.Sleep(time.Second)
	r4 := notifier.NewReceiver(-1)
	<-r4.C
	r4.Stop()

	notifier2 := new(Notifier)
	r5 := notifier2.NewReceiver(10 * time.Millisecond)
	<-r5.C
	r5.Stop()
}
