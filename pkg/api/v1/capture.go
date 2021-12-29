package v1

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/api/internal/rest"
)

// CapturesGetter has a method to return a CaptureInterface.
type CapturesGetter interface {
	Captures() CaptureInterface
}

// CaptureInterface has methods to work with Capture items.
// We can also mock the capture operations by implement this interface.
type CaptureInterface interface {
	List(ctx context.Context) (*[]model.Capture, error)
}

// captures implements CaptureInterface
type captures struct {
	client rest.CDCRESTInterface
}

// newCaptures returns captures
func newCaptures(c *ApiV1Client) *captures {
	return &captures{
		client: c.RESTClient(),
	}
}

// List returns the list of captures
func (c *captures) List(ctx context.Context) (result *[]model.Capture, err error) {
	result = &[]model.Capture{}
	err = c.client.Get().
		WithURI("captures").
		Do(ctx).
		Into(result)
	return
}
