package v1

import (
	"context"
	"fmt"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/api/internal/rest"
)

// ChangfeedGetter has a method to return a ChangfeedInterface.
type ChangefeedsGetter interface {
	Changefeeds() ChangefeedInterface
}

// ChangefeedInterface has methods to work with Changfeed items.
// We can also mock the changefeed operations by implement this interface.
type ChangefeedInterface interface {
	Get(ctx context.Context, name string) (*model.ChangefeedDetail, error)
	List(ctx context.Context) (*[]model.ChangeFeedInfo, error)
}

// changefeeds implements ChangefeedInterface
type changefeeds struct {
	client rest.CDCRESTInterface
}

// newChangefeeds returns changefeeds
func newChangefeeds(c *ApiV1Client) *changefeeds {
	return &changefeeds{
		client: c.RESTClient(),
	}
}

// Get takes name of the changfeed, and returns the corresponding changfeed object,
// and an error if there is any.
func (c *changefeeds) Get(ctx context.Context, name string) (result *model.ChangefeedDetail, err error) {
	result = &model.ChangefeedDetail{}
	u := fmt.Sprintf("changefeeds/%s", name)
	err = c.client.Get().
		WithURI(u).
		Do(ctx).
		Into(result)
	return
}

// List returns the list of changefeeds
func (c *changefeeds) List(ctx context.Context) (result *[]model.ChangeFeedInfo, err error) {
	result = &[]model.ChangeFeedInfo{}
	err = c.client.Get().
		WithURI("changefeeds").
		Do(ctx).
		Into(result)
	return
}
