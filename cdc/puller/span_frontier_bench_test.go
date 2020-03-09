package puller

import (
	"fmt"
	"testing"

	"github.com/pingcap/ticdc/pkg/util"
)

func toCMPBytes(i int) []byte {
	s := fmt.Sprintf("%09d", i)
	return []byte(s)
}

func BenchmarkSpanFrontier(b *testing.B) {
	tests := []struct {
		name string
		n    int
	}{
		{name: "5k", n: 5000},
		{name: "10k", n: 10_000},
		{name: "50k", n: 50_000},
		{name: "100k", n: 100_000},
	}

	for _, test := range tests {
		n := test.n

		b.Run(test.name, func(b *testing.B) {
			spans := make([]util.Span, 0, n)
			for i := 0; i < n; i++ {
				span := util.Span{
					Start: toCMPBytes(i),
					End:   toCMPBytes(i + 1),
				}
				spans = append(spans, span)
			}

			f := makeSpanFrontier(spans...)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				f.Forward(spans[i%n], uint64(i))
			}
		})
	}
}
