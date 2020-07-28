package org.pingcap.ticdc.cdc.value;

/**
 * https://docs.pingcap.com/zh/tidb/v4.0/ticdc-open-protocol#message-%E6%A0%BC%E5%BC%8F%E5%AE%9A%E4%B9%89
 */
public enum TicdcEventType {
    rowChange, ddl, resolved
}
