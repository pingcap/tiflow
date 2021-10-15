package openapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/pingcap/check"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

var _ = check.Suite(&zapLoggerSuite{})

type zapLoggerSuite struct{}

func TestZapLogger(t *testing.T) {
	check.TestingT(t)
}

func (t *zapLoggerSuite) TestZapLogger(c *check.C) {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/something", nil)
	rec := httptest.NewRecorder()
	ctx := e.NewContext(req, rec)
	h := func(ctx echo.Context) error {
		return ctx.String(http.StatusOK, "")
	}
	obs, logs := observer.New(zap.DebugLevel)
	logger := zap.New(obs)
	err := ZapLogger(logger)(h)(ctx)
	c.Assert(err, check.IsNil)

	logFields := logs.All()[0].ContextMap()

	c.Assert(logFields["method"], check.Equals, "GET")
	c.Assert(logFields["request"], check.Equals, "GET /something")
	c.Assert(logFields["status"], check.Equals, int64(200))
	c.Assert(logFields["duration"], check.NotNil)
	c.Assert(logFields["host"], check.NotNil)
	c.Assert(logFields["protocol"], check.NotNil)
	c.Assert(logFields["remote_ip"], check.NotNil)
	c.Assert(logFields["user_agent"], check.NotNil)
	c.Assert(logFields["request"], check.NotNil)
	c.Assert(logFields["error"], check.IsNil)
}
