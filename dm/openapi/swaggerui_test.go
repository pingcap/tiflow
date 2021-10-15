package openapi

import (
	"strings"
	"testing"

	"github.com/pingcap/check"
)

var _ = check.Suite(&swaggerUISuite{})

type swaggerUISuite struct{}

func TestNewSwaggerDocUI(t *testing.T) {
	check.TestingT(t)
}

func (t *swaggerUISuite) TestGetSwaggerHTML(c *check.C) {
	html, err := GetSwaggerHTML(NewSwaggerConfig("/api/v1/docs/dm.json", ""))
	c.Assert(err, check.IsNil)
	c.Assert(strings.Contains(html, "<title>API documentation</title>"), check.Equals, true)
}

func (t *swaggerUISuite) TestGetSwaggerJSON(c *check.C) {
	_, err := GetSwaggerJSON()
	c.Assert(err, check.IsNil)
}
