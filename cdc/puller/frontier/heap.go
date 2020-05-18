// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package frontier

type minTsHeap struct {
	root *node
	min  *node

	dirty bool
}

func (h *minTsHeap) getMin() *node {
	if h.dirty {
		h.consolidate()
		h.dirty = false
	}
	return h.min
}

func (h *minTsHeap) insert(x *node) {
	h.addToRoot(x)
	if h.min == nil || h.min.ts > x.ts {
		h.min = x
	}
}

func (h *minTsHeap) increaseTs(x *node, ts uint64) {
	if x == h.min {
		h.dirty = true
	}

	x.ts = ts
	child := x.children
	cascadingCut := false
	isLast := child == nil

	for !isLast {
		next := child.right
		isLast = next == x.children

		if child.ts < x.ts {
			h.removeChildren(x, child)
			h.addToRoot(child)
			if x.marked {
				cascadingCut = true
			}
			x.marked = true
		}

		child = next
	}

	if cascadingCut {
		h.cascadingCut(x)
	}
}

func (h *minTsHeap) cascadingCut(x *node) {
	x.marked = false
	for p := x.parent; p != nil; p = p.parent {
		h.removeChildren(p, x)
		h.addToRoot(x)
		x.marked = false
		if !p.marked {
			p.marked = true
			break
		}
		x = p
	}
}

// The upper bound of fibonacci heap's rank is log1.618(n).
// So if we allow 1 << 32 (4294967296) spans tracked by this heap,
// we can know the size of consolidate table is around 46.09545510610244.
const consolidateTableSize = 47

func (h *minTsHeap) consolidate() {
	var table [consolidateTableSize]*node
	x := h.root
	maxOrder := 0
	h.min = h.root

	for {
		y := x
		x = x.right
		z := table[y.rank]
		for z != nil {
			table[y.rank] = nil
			if y.ts > z.ts {
				y, z = z, y
			}
			h.addChildren(y, z)
			z = table[y.rank]
		}
		table[y.rank] = y
		if y.rank > maxOrder {
			maxOrder = y.rank
		}

		if x == h.root {
			break
		}
	}

	h.root = nil
	for _, n := range table {
		if n == nil {
			continue
		}
		if h.min.ts > n.ts {
			h.min = n
		}
		h.addToRoot(n)
	}
}

func (h *minTsHeap) addChildren(root, x *node) {
	root.children = h.insertInto(root.children, x)
	root.rank++
	x.parent = root
}

func (h *minTsHeap) removeChildren(root, x *node) {
	root.children = h.cutFrom(root.children, x)
	root.rank--
	x.parent = nil
}

func (h *minTsHeap) addToRoot(x *node) {
	h.root = h.insertInto(h.root, x)
}

func (h *minTsHeap) insertInto(head *node, x *node) *node {
	if head == nil {
		x.left = x
		x.right = x
	} else {
		head.left.right = x
		x.right = head
		x.left = head.left
		head.left = x
	}
	return x
}

func (h *minTsHeap) cutFrom(head *node, x *node) *node {
	if x.right == x {
		x.right = nil
		x.left = nil
		return nil
	}

	x.right.left = x.left
	x.left.right = x.right
	ret := x.right
	x.right = nil
	x.left = nil
	if head == x {
		return ret
	}
	return head
}
