package notify

import (
	"context"
	"time"

	"github.com/pingcap/check"
)

type notifySuite struct{}

var _ = check.Suite(&notifySuite{})

func (s *notifySuite) TestNotifyHub(c *check.C) {
	hub := NewNotifyHub()
	testName1 := "test1"
	notifier := hub.GetNotifier(testName1)
	rCh1, close1 := notifier.Receiver()
	rCh2, close2 := notifier.Receiver()
	rCh3, close3 := notifier.Receiver()
	go func() {
		for i := 0; i < 5; i++ {
			time.Sleep(time.Second)
			notifier.Notify(context.Background())
		}
	}()
	<-rCh1
	close1()
	<-rCh2
	<-rCh3

	close2()
	close3()
	c.Assert(len(notifier.notifyChs), check.Equals, 0)
	time.Sleep(time.Second)
	rCh4, close4 := notifier.Receiver()
	<-rCh4
	close4()
}
