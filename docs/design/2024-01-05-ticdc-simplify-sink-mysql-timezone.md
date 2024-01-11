# Simplify Sink to MySQL Timezone Handling

- Author(s): [zhangjinpeng1987](https://github.com/zhangjinpeng1987)
- Tracking Issue(s):

## Goals

- Simplify CDC's sink to MySQL timezone handling logic, eliminate timezone related data correctness risk
- Make CDC a timezone insensitive component, reduce users' timezone maintenance burden for CDC

## Problem: How does CDC handle timezone now?

<img src="../media/cdc-timezone.png?sanitize=true" alt="architecture" width="600"/>

For a sink-to-mysql changefeed, CDC mounters use CDC server configured timezone to decode timezone sensitive columns like `timestamp`. CDC sink part (write changes to downstream MySQL) will try to use the specified timezone included in the sink-uri. If the timezone is not provided in the sink-uri, it will use the cdc server configured timezone. But essentially, if we want CDC replicate data correctly for timezone sensitive columns like `timestamp`, CDC mounters and CDC sink should use the same timezone to decode and write data changes received from TiKV. Following pseudo code show current CDC's timezone handling logic:

```
mouter.time_zone = cdc-server.timezone

if sink-uri.timezone is not empty (case 1):
    // For example: sink-uri="mysql://user:pwd@tcp(127.0.0.1:3306)/db?time-zone=utc".

    sink.time_zone = sink-uri.timezone
    if sink-uri.timezone != cdc-server.timezone:
        return error. // because mounter and sink may use different timezone which can cause data correctness issue
    if downstream MySQL doesn't installed related timezone:
        users should follow https://dev.mysql.com/doc/refman/8.0/en/mysql-tzinfo-to-sql.html to install them.

else if sink-uri.timezone == "" (case 2):
    // For example: sink-uri="mysql://root:pwd@tcp(127.0.0.1:3306)/db?time-zone=''".
    // In this case, sink.timezone depends on downstream MySQL's time_zone setting whose
    // default value is `SYSTEM`. We require users set same timezone for ticdc servers and downstream
    // MySQL servers to make sure timezone awared column type like `timestamp` is
    // replicated correctly. **If the user dont recognize this case, there is a risk that
    // the replicated `timestamp` value is incorrect, at the same time set timezone for
    // machine might affect other application in the same machine**.

    sink.timezone = downstream-MySQL.time_zone

else if sink-uri.time_zone is nil (case 3):
    // In this case, sink will use cdc server configured timezone. If the downstream MySQL
    // has not installed related timezone, users should follow
    // https://dev.mysql.com/doc/refman/8.0/en/mysql-tzinfo-to-sql.html to install them.

    sink.timezone = cdc-server.timezone
```

From above description we can tell some potential problems with existing timezone handling in CDC:

- Case 2 has a correctness risk for `TIMESTAMP` column types when the users don't set downstram MySQL as the same timezone with CDC server. But in the case of cross region replication, databases and machines in different regions may have different default timezone setting.
- Users should carefully treat timezone setting for cdc servers, sink-uri and downstream MySQL/TiDB, this is a maintenance burden for users.

## Proposal Changes

### CDC internal changes

- Mounters always use UTC timezone to decode data changes received from TiKV.
- MySQL sink always set session variable `set time_zone = '+00:00'` when replicate data to downstream MySQL.
- Mounters and MySQL sink ignore timezone settings in cdc-server and sink-uri.

### Changes from users' perspective

- Users don't need to adjust cdc-server timezone, sink-uri timezone and donwstream MySQL timezone for CDC.

### Upgrade & Compatibility

- Because MySQL 5.6/5.7/8.0 and TiDB support `set time_zone = '+00:00'` by default, there is no compatibility issue or any required changes for exiting changefeed when upgrade CDC to new version.

## Appendix

### How MySQL and TiDB handle timezone sensitive column types

Currently, there is just one timezone sensitive column type `TIMESTAMP`, both TiDB and MySQL will convert it as UTC values and then store them, and convert back from UTC to the current time zone for retrieval. For other time column types like `DATE` and `DATETIME`, both MySQL and TiDB treat them as other types like int, they are not timezone sensitive types, application layer should handle them correctly.

_MySQL converts TIMESTAMP values from the current time zone to UTC for storage, and back from UTC to the current time zone for retrieval. (This does not occur for other types such as DATETIME.) By default, the current time zone for each connection is the server's time. The time zone can be set on a per-connection basis. As long as the time zone setting remains constant, you get back the same value you store. If you store a TIMESTAMP value, and then change the time zone and retrieve the value, the retrieved value is different from the value you stored. This occurs because the same time zone was not used for conversion in both directions. The current time zone is available as the value of the time_zone system variable._

### MySQL binlog cross region replication case

According to https://dev.mysql.com/doc/refman/8.0/en/replication-features-timezone.html, when using MySQL binlog to replicate data across regions, MySQL suggests users explicitly set related time_zone for both source and replica. The reason is because when the binlog format is `statement` and some time zone sensitive functions like `NOW()` and `FROM_UNIXTIME()` may result in inconsistent value. But TiCDC is more like a row format binlog, it receiving row level changes from TiKV and decode them and repilcate downstream, and CDC has no such issue.

[NEED TO VERIFY] For row format binlog, `TIMESTAMP` column UTC values are recored and replicated, and the downstream MySQL decode it and write it with the same timezone to make sure it is correct (same as our proposal way above).
