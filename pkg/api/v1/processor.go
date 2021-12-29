package v1

import (
	"context"
	"fmt"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/api/internal/rest"
)

// ProcessorsGetter has a method to return a ProcessorInterface.
type ProcessorsGetter interface {
	Processors() ProcessorInterface
}

// ProcessorInterface has methods to work with Processor items.
// We can also mock the processor operations by implement this interface.
type ProcessorInterface interface {
	Get(ctx context.Context, changefeedId, captureId string) (*model.ProcessorDetail, error)
	List(ctx context.Context) (*[]model.ProcessorCommonInfo, error)
}

// processors implements ProcessorInterface
type processors struct {
	client rest.CDCRESTInterface
}

// newProcessors returns processors
func newProcessors(c *ApiV1Client) *processors {
	return &processors{
		client: c.RESTClient(),
	}
}

// Get takes name of the processor, and returns the corresponding processor object,
// and an error if there is any.
func (c *processors) Get(ctx context.Context, changefeedId, captureId string) (result *model.ProcessorDetail, err error) {
	result = &model.ProcessorDetail{}
	u := fmt.Sprintf("processors/%s/%s", changefeedId, captureId)
	err = c.client.Get().
		WithURI(u).
		Do(ctx).
		Into(result)
	return
}

// List returns the list of processors
func (c *processors) List(ctx context.Context) (result *[]model.ProcessorCommonInfo, err error) {
	result = &[]model.ProcessorCommonInfo{}
	err = c.client.Get().
		WithURI("processors").
		Do(ctx).
		Into(result)
	return
}
