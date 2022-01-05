package runtime

import (
	"testing"

	"github.com/hanfei1991/microcosm/model"
	"github.com/stretchr/testify/require"
)

func TestResourceSnapshot(t *testing.T) {
	t.Parallel()
	r := &Runtime{
		tasks: map[model.ID]*taskContainer{
			1: {tru: NewSimpleTRU(model.Benchmark)},
			2: {tru: NewSimpleTRU(model.Benchmark)},
		},
	}
	resc := r.Resource()
	require.Equal(t, 1, len(resc))
	require.GreaterOrEqual(t, resc[model.Benchmark], 2)
	require.LessOrEqual(t, resc[model.Benchmark], 6)
}
