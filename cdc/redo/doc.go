//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

/*
Package redo provide a redo log for cdc.
Each log file contains batch of RowChangedEvent or DDLEvent records wrote into different file with defaultMaxLogSize 64 MB. If larger than 64 MB will auto rotated to a new file.
A record has a length field and a logical Log data. The length field is a 64-bit packed structure holding the length of the remaining logical Log data in its lower
56 bits and its physical padding in the first three bits of the most significant byte. Each record is 8-byte aligned so that the length field is never torn.
*/
package redo
