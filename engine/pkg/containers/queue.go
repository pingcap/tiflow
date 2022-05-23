// Copyright 2022 PingCAP, Inc.
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

package containers

// Queue abstracts a generics FIFO queue, which is thread-safe
type Queue[T any] interface {
	Push(elem T)
	Pop() (T, bool)
	Peek() (T, bool)
	Size() int
}
