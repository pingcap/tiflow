package sorter

import "github.com/pingcap/ticdc/cdc/model"

type sortItem struct {
	entry     *model.PolymorphicEvent
	fileIndex int
	data      interface{}
}

type sortHeap []*sortItem

func (h sortHeap) Len() int { return len(h) }
func (h sortHeap) Less(i, j int) bool {
	if h[i].entry.CRTs == h[j].entry.CRTs {
		if h[j].entry.RawKV != nil && h[j].entry.RawKV.OpType == model.OpTypeResolved && h[i].entry.RawKV.OpType != model.OpTypeResolved {
			return true
		}
		if h[i].entry.RawKV != nil && h[i].entry.RawKV.OpType == model.OpTypeDelete && h[j].entry.RawKV.OpType != model.OpTypeDelete {
			return true
		}
	}
	return h[i].entry.CRTs < h[j].entry.CRTs
}
func (h sortHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *sortHeap) Push(x interface{}) {
	*h = append(*h, x.(*sortItem))
}
func (h *sortHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return x
}
