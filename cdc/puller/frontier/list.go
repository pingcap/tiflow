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

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"strings"

	_ "unsafe" // required by go:linkname
)

const (
	maxHeight = 12
)

type skipListNode struct {
	key   []byte
	value *fibonacciHeapNode

	nexts []*skipListNode
}

// Key is the key of the node
func (s *skipListNode) Key() []byte {
	return s.key
}

// Value is the value of the node
func (s *skipListNode) Value() *fibonacciHeapNode {
	return s.value
}

// Next returns the next node in the list
func (s *skipListNode) Next() *skipListNode {
	return s.nexts[0]
}

type seekResult []*skipListNode

// Next points to the next seek result
func (s seekResult) Next() {
	next := s.Node().Next()
	for i := range next.nexts {
		s[i] = next
	}
}

// Node returns the node point by the seek result
func (s seekResult) Node() *skipListNode {
	if len(s) == 0 {
		return nil
	}
	return s[0]
}

type skipList struct {
	head   skipListNode
	height int
}

func newSpanList() *skipList {
	l := new(skipList)
	l.head.nexts = make([]*skipListNode, maxHeight)
	return l
}

func (l *skipList) randomHeight() int {
	h := 1
	for h < maxHeight && fastrand() < uint32(math.MaxUint32)/4 {
		h++
	}
	return h
}

//go:linkname fastrand runtime.fastrand
func fastrand() uint32

// Seek returns the seek result
// the seek result is a slice of nodes,
// Each element in the slice represents the nearest(left) node to the target value at each level of the skip list.
func (l *skipList) Seek(key []byte) seekResult {
	head := &l.head
	current := head
	result := make(seekResult, maxHeight)

LevelLoop:
	for level := l.height - 1; level >= 0; level-- {
		for {
			next := current.nexts[level]
			if next == nil {
				result[level] = current
				continue LevelLoop
			}
			cmp := bytes.Compare(key, next.key)
			if cmp < 0 {
				result[level] = current
				continue LevelLoop
			}
			if cmp == 0 {
				for ; level >= 0; level-- {
					result[level] = next
				}
				return result
			}
			current = next
		}
	}

	return result
}

// InsertNextToNode insert the specified node after the seek result
func (l *skipList) InsertNextToNode(seekR seekResult, key []byte, value *fibonacciHeapNode) {
	if seekR.Node() != nil && !nextTo(seekR.Node(), key) {
		log.Fatal("the InsertNextToNode function can only append node to the seek result.")
	}
	height := l.randomHeight()
	if l.height < height {
		l.height = height
	}
	n := &skipListNode{
		key:   key,
		value: value,
		nexts: make([]*skipListNode, height),
	}

	for level := 0; level < height; level++ {

		prev := seekR[level]
		if prev == nil {
			prev = &l.head
		}
		n.nexts[level] = prev.nexts[level]
		prev.nexts[level] = n
	}
}

// Insert inserts the specified node
func (l *skipList) Insert(key []byte, value *fibonacciHeapNode) {
	seekR := l.Seek(key)
	l.InsertNextToNode(seekR, key, value)
}

// Remove removes the specified node after the seek result
func (l *skipList) Remove(seekR seekResult, toRemove *skipListNode) {
	seekCurrent := seekR.Node()
	if seekCurrent == nil || seekCurrent.Next() != toRemove {
		log.Fatal("the Remove function can only remove node right next to the seek result.")
	}
	for i := range toRemove.nexts {
		seekR[i].nexts[i] = toRemove.nexts[i]
	}
}

// First returns the first node in the list
func (l *skipList) First() *skipListNode {
	return l.head.Next()
}

// Entries visit all the nodes in the list
func (l *skipList) Entries(fn func(*skipListNode) bool) {
	for node := l.First(); node != nil; node = node.Next() {
		if cont := fn(node); !cont {
			return
		}
	}
}

// String implements fmt.Stringer interface.
func (l *skipList) String() string {
	var buf strings.Builder
	l.Entries(func(node *skipListNode) bool {
		buf.WriteString(fmt.Sprintf("[%s] ", node.key))
		return true
	})
	return buf.String()
}

// nextTo check if the key is right next to the node in list.
// the specified node is a node in the list.
// the specified key is a bytes to check position.
// return true if the key is right next to the node.
func nextTo(node *skipListNode, key []byte) bool {
	cmp := bytes.Compare(node.key, key)
	switch {
	case cmp == 0:
		return true
	case cmp > 0:
		return false
	}
	// cmp must be less than 0 here
	next := node.nexts[0]
	if next == nil {
		// the node is the last node in the list
		// we can insert the key after the last node.
		return true
	}
	if bytes.Compare(next.key, key) <= 0 {
		// the key of next node is less or equal to the specified key,
		// this specified key should be inserted after the next node.
		return false
	}
	// the key is between with the node and the next node.
	return true
}
