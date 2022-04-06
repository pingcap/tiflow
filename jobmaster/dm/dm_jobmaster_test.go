package dm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

func TestDMJobmasterSuite(t *testing.T) {
	suite.Run(t, new(testDMJobmasterSuite))
}

type testDMJobmasterSuite struct {
	suite.Suite
}

func (t *testDMJobmasterSuite) SetupSuite() {
	TaskNormalInterval = time.Hour
	TaskErrorInterval = 100 * time.Millisecond
	WorkerNormalInterval = time.Hour
	WorkerErrorInterval = 100 * time.Millisecond
}
