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

type fibonacciHeapNode struct {
	key uint64

	left     *fibonacciHeapNode
	right    *fibonacciHeapNode
	children *fibonacciHeapNode
	parent   *fibonacciHeapNode
	rank     int
	marked   bool
}

type fibonacciHeap struct {
	root *fibonacciHeapNode
	min  *fibonacciHeapNode

	dirty bool
}

// GetMinKey returns the minimum key in the heap
func (h *fibonacciHeap) GetMinKey() uint64 {
	if h.dirty {
		h.consolidate()
		h.dirty = false
	}
	return h.min.key
}

// Insert inserts a new node into the heap and returns this new node
func (h *fibonacciHeap) Insert(key uint64) *fibonacciHeapNode {
	x := &fibonacciHeapNode{key: key}
	h.addToRoot(x)
	if h.min == nil || h.min.key > x.key {
		h.min = x
	}
	return x
}

// Remove removes a node from the heap
func (h *fibonacciHeap) Remove(x *fibonacciHeapNode) {
	if x == h.min {
		h.dirty = true
	}

	child := x.children
	isLast := child == nil

	for !isLast {
		next := child.right
		isLast = next == x.children
		h.removeChildren(x, child)
		h.addToRoot(child)
		child = next
	}

	parent := x.parent
	if parent != nil {
		h.removeChildren(parent, x)
		if parent.marked {
			h.cascadingCut(parent)
		} else {
			parent.marked = true
		}
	} else {
		h.cutFromRoot(x)
	}
}

// UpdateKey updates the key of the node in the heap
func (h *fibonacciHeap) UpdateKey(x *fibonacciHeapNode, key uint64) {
	switch {
	case x.key == key:
		return
	case x.key > key:
		h.decreaseKey(x, key)
	case x.key < key:
		h.increaseKey(x, key)
	}
}

func (h *fibonacciHeap) increaseKey(x *fibonacciHeapNode, key uint64) {
	if x == h.min {
		h.dirty = true
	}

	x.key = key
	child := x.children
	cascadingCut := false
	isLast := child == nil

	for !isLast {
		next := child.right
		isLast = next == x.children

		if child.key < x.key {
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

func (h *fibonacciHeap) decreaseKey(x *fibonacciHeapNode, key uint64) {
	x.key = key
	parent := x.parent
	if parent != nil && parent.key > x.key {
		h.removeChildren(parent, x)
		h.addToRoot(x)
		if parent.marked {
			h.cascadingCut(parent)
		} else {
			parent.marked = true
		}
	}
	if x.parent == nil && h.min.key > key {
		h.min = x
	}
}

func (h *fibonacciHeap) cascadingCut(x *fibonacciHeapNode) {
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

func (h *fibonacciHeap) consolidate() {
	var table [consolidateTableSize]*fibonacciHeapNode
	x := h.root
	maxOrder := 0
	h.min = h.root

	for {
		y := x
		x = x.right
		z := table[y.rank]
		for z != nil {
			table[y.rank] = nil
			if y.key > z.key {
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
		if h.min.key > n.key {
			h.min = n
		}
		h.addToRoot(n)
	}
}

func (h *fibonacciHeap) addChildren(root, x *fibonacciHeapNode) {
	root.children = h.insertInto(root.children, x)
	root.rank++
	x.parent = root
}

func (h *fibonacciHeap) removeChildren(root, x *fibonacciHeapNode) {
	root.children = h.cutFrom(root.children, x)
	root.rank--
	x.parent = nil
}

func (h *fibonacciHeap) addToRoot(x *fibonacciHeapNode) {
	h.root = h.insertInto(h.root, x)
}

func (h *fibonacciHeap) cutFromRoot(x *fibonacciHeapNode) {
	h.root = h.cutFrom(h.root, x)
}

func (h *fibonacciHeap) insertInto(head *fibonacciHeapNode, x *fibonacciHeapNode) *fibonacciHeapNode {
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

func (h *fibonacciHeap) cutFrom(head *fibonacciHeapNode, x *fibonacciHeapNode) *fibonacciHeapNode {
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
