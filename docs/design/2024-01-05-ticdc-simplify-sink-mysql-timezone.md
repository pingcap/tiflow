# Simplify Sink to MySQL Timezone Handling

- Author(s): [zhangjinpeng1987](https://github.com/zhangjinpeng1987)
- Tracking Issue(s): 

## Goals

- Simplify CDC's sink to MySQL timezone handling logic, eliminate timezone related data correctness risk
- Make CDC a timezone insensitive component, reduce users' timezone maintenance burden for CDC

## Problem: How does CDC handle timezone now?

<img src="../media/cdc-timezone.png?sanitize=true" alt="architecture" width="600"/>

For a sink-to-mysql changefeed, CDC mounters use CDC server configured timezone to decode timezone awared columns like `timestamp`. CDC sink part (write changes to downstream MySQL) will try to use the specified timezone included in the sink-uri first. If the timezone is not provided in the sink-uri, it will use the cdc server configured timezone. Following pseudo code show more details:

```
if sink-uri.time_zone is not empty (case 1):
    // sink-uri="user:pwd@tcp(127.0.0.1:3306)/db?time-zone=utc".
    sink.time_zone = sink-uri.timezone
    if sink-uri.timezone != cdc.server.timezone:
        
    not equal to CDC server configured timezone, return error.
    If the downstream MySQL dont installed related timezone, users should follow
    https://dev.mysql.com/doc/refman/8.0/en/mysql-tzinfo-to-sql.html to install them.
else if sink-uri.time_zone == "" (case 2):
    // sink-uri="root:pwd@tcp(127.0.0.1:3306)/db?time-zone=''".
    sink do not explicitly set time_zone session variable, it depends on downstream         
    MySQLs default timezone value which typically is `SYSTEM`.
    In this case, we require user set same timezone for ticdc servers and downstream
    MySQL servers to make sure timezone awared column type like `timestamp` is
    replicated correctly. **If the user dont recognize this case, there is a risk that
    the replicated `timestamp` value is incorrect, at the same time set timezone for   
    machine might affect other application in the same machine**. 
else if sink-uri.time_zone is nil and cdc server timezone is configured (case 3){
    In this case, sink will use cdc server configured timezone. If the downstream MySQL
    has not installed related timezone, users should follow 
    https://dev.mysql.com/doc/refman/8.0/en/mysql-tzinfo-to-sql.html to install them.
```

From above description we can tell that:
- 
