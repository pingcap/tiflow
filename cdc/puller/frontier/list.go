package frontier

import (
	"bytes"
	"math"

	_ "unsafe" // required by go:linkname
)

const (
	maxHeight = 12
)

type spanList struct {
	head   node
	height int
}

func (n *node) next() *node {
	return n.nexts[0]
}

func (l *spanList) init() {
	l.head.nexts = make([]*node, maxHeight)
}

func (l *spanList) insert(n *node) {
	head := &l.head
	lsHeight := l.height
	var prev [maxHeight + 1]*node
	var next [maxHeight + 1]*node
	prev[lsHeight] = head

	for i := lsHeight - 1; i >= 0; i-- {
		prev[i], next[i] = l.findSpliceForLevel(n.start, prev[i+1], i)
	}
	height := l.randomHeight()
	n.nexts = make([]*node, height)
	if height > l.height {
		l.height = height
	}

	for i := 0; i < height; i++ {
		n.nexts[i] = next[i]
		if prev[i] == nil {
			prev[i] = &l.head
		}
		prev[i].nexts[i] = n
	}
}

func (l *spanList) randomHeight() int {
	h := 1
	for h < maxHeight && fastrand() < uint32(math.MaxUint32)/4 {
		h++
	}
	return h
}

//go:linkname fastrand runtime.fastrand
func fastrand() uint32

func (l *spanList) findSpliceForLevel(key []byte, prev *node, level int) (*node, *node) {
	for {
		next := prev.nexts[level]
		if next == nil {
			return prev, nil
		}
		cmp := bytes.Compare(next.start, key)
		if cmp >= 0 {
			return prev, next
		}
		prev = next
	}
}

func (l *spanList) seek(start []byte) *node {
	if l.head.next() == nil {
		return nil
	}

	head := &l.head
	prev := head
	level := l.height - 1

	for {
		next := prev.nexts[level]
		if next != nil {
			cmp := bytes.Compare(start, next.start)
			if cmp > 0 {
				prev = next
				continue
			}
			if cmp == 0 {
				return next
			}
		}
		if level > 0 {
			level--
			continue
		}
		break
	}

	// The seek key is smaller than all keys in the list.
	if prev == head {
		return head.next()
	}
	return prev
}
