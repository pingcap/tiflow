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

There are three types of log file: meta log file, row log file, ddl log file.
meta file used to store common.LogMeta info (CheckPointTs, ResolvedTs), atomic updated is guaranteed. A rotated file writer is used for other log files.
All files will flush to disk or upload to s3 if enabled every defaultFlushIntervalInMs 1000ms or file size larger than defaultMaxLogSize 64 MB by default.
The log file name is formatted as CaptureID_ChangeFeedID_CreateTime_FileType_MaxCommitTSOfAllEventInTheFile.log if safely wrote or end up with .log.tmp is not.
meta file name is like CaptureID_ChangeFeedID_meta.meta

Each log file contains batch of model.RedoRowChangedEvent or model.RedoDDLEvent records wrote into different file with defaultMaxLogSize 64 MB.
If larger than 64 MB will auto rotated to a new file.
A record has a length field and a logical Log data. The length field is a 64-bit packed structure holding the length of the remaining logical Log data in its lower
56 bits and its physical padding in the first three bits of the most significant byte. Each record is 8-byte aligned so that the length field is never torn.

When apply redo log from cli, will select files in the specific dir to open base on the
startTs, endTs send from cli or download logs from s3 first is enabled, then sort the event
records in each file base on commitTs and startTs, after sorted, the new sort file name
should be as CaptureID_ChangeFeedID_CreateTime_FileType_MaxCommitTSOfAllEventInTheFile.log.sort.
*/
package redo
