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

// Package gc privodes TiDB GC related utilities.
//
// Following graph shows how TiCDC manipulates GC safepoints.
//
// ```
//
//	          ┌───┐          ┌───┐          ┌──┐                   ┌─────┐
//	          │CLI│          │API│          │PD│                   │Owner│
//	          └─┬─┘          └─┬─┘          └┬─┘                   └──┬──┘
//	            │              │             │                        │
//	╔═══════════╪═════╤════════╪═════════════╪════════════════════════╪════════════╗
//	║ "CLI OR API?"   │  CLI   │             │                        │            ║
//	╟─────────────────┘        │             │                        │            ║
//	║           │   service GC safepoint     │                        │            ║
//	║           │   "cdc-creating-<ID>"      │                        │            ║
//	║           │──────────────────────────>┌┴┐                       │            ║
//	║           │              │            │ │                       │            ║
//	║           │    Create changefeed      │ │                       │            ║
//	║           │──────────────────────────>│ │                       │            ║
//	╠═══════════╪══════════════╪════════════╪═╪═══════════════════════╪════════════╣
//	║  API      │              │            │ │                       │            ║
//	║           │              │        Create changefeed API         │            ║
//	║           │              │─────────────────────────────────────>│            ║
//	║           │              │            │ │                       │            ║
//	║           │              │            │ │   service GC safepoint│            ║
//	║           │              │            │ │   "cdc-creating-<ID>" │            ║
//	║           │              │            │ ┌─┐ <────────────────────            ║
//	║           │              │            │ │ │                     │            ║
//	║           │              │            │ │ │  Create changefeed  │            ║
//	║           │              │            │ │ │ <────────────────────            ║
//	╚═══════════╪══════════════╪════════════╪═╪═╪═════════════════════╪════════════╝
//	            │              │            │ │ │                     │
//	            │              │            │ │ │                     │  ╔═════════════════╗
//	            │              │            │ │ │ GC safepoint "ticdc"│  ║using the minimal║
//	            │              │            │ │ │ <────────────────────  ║checkpoint among ║
//	            │              │            │ │ │                     │  ║all changefeeds  ║
//	            │              │            │ │ │                     │  ╚═════════════════╝
//	            │              │            │ │ │Remove GC safepoint  │
//	            │              │            │ │ │"cdc-creating-<ID>"  │
//	            │              │            └┬└─┘ <────────────────────
//	            │              │             │                        │
//	            │              │             │                        │────┐
//	            │              │             │                        │    │ Start changefeed
//	            │              │             │                        │<───┘
//	          ┌─┴─┐          ┌─┴─┐          ┌┴─┐                   ┌──┴──┐
//	          │CLI│          │API│          │PD│                   │Owner│
//	          └───┘          └───┘          └──┘                   └─────┘
//
// ```
//
// When CLI/API creates a changefeed, it registers a service GC safepoint
// "ticdc-creating-<ID>" to cover the gap between creating changefeed and
// handling changefeed.
//
// TiCDC Owner removes the service GC safepoint after new changefeed checkpoint
// is included in the "ticdc" service GC safepoint.
package gc
