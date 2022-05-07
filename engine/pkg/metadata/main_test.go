package metadata

import (
	"testing"

	"github.com/hanfei1991/microcosm/pkg/leakutil"
)

func TestMain(m *testing.M) {
	leakutil.SetUpLeakTest(m)
}
