// Copyright 2021 PingCAP, Inc.
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

package printinit

// we expect this helper function to be called at very early stage of initialization, because this package has no
// imports.
// There are some failing CI indicating it takes at least 10 seconds to start a DM-master, we need to know if the OS
// spawn processes too slowly or some init() are slow to finish.
func init() {
	println("init now")
}
