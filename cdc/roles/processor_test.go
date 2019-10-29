package roles

import (
	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type processorSuite struct{}

type mockTSRWriter struct {
	resolvedTS   uint64
	checkpointTS uint64
}

func (s *mockTSRWriter) WriteResolvedTS(resolvedTS uint64) error {
	log.Info("write", zap.Uint64("localResolvedTS", resolvedTS))
	s.resolvedTS = resolvedTS
	return nil
}

func (s *mockTSRWriter) WriteCheckpointTS(checkpointTS uint64) error {
	log.Info("write", zap.Uint64("checkpointTS", checkpointTS))
	s.checkpointTS = checkpointTS
	return nil
}

func (s *mockTSRWriter) ReadGlobalResolvedTS() (uint64, error) {
	return s.resolvedTS, nil
}

var _ = check.Suite(&processorSuite{})
