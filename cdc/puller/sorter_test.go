package puller

import (
	"context"
	"github.com/pingcap/check"
	"golang.org/x/sync/errgroup"
)

const (
	numProducers = 10
	eventCountPerProducer = 10000000
)

type sorterSuite struct{}

var _ = check.Suite(&sorterSuite{})

func (s *sorterSuite) testSorter(c *check.C, sorter EventSorter) {
	errg, ctx := errgroup.WithContext(context.Background())
	errg.Go(func() error {
		return sorter.Run(ctx)
	})

	// launch the producers
	for i := 0; i < eventCountPerProducer; i++ {
		errg.Go(func() error {

			return nil
		})
	}
}
