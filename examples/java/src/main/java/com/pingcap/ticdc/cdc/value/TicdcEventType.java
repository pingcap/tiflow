/*
 * Copyright 2020 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.ticdc.cdc.value;

/**
 * https://docs.pingcap.com/zh/tidb/v4.0/ticdc-open-protocol#message-%E6%A0%BC%E5%BC%8F%E5%AE%9A%E4%B9%89
 */
public enum TicdcEventType {
    rowChange, ddl, resolved
}
