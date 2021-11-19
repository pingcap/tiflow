
## Running

```bash
./binlog-event-blackhole -addr=127.0.0.1:3306 -u root -server-id=101 -binlog-name=mysql-bin.000003 -mode=1
```

```
  -L string
    	log level: debug, info, warn, error, fatal (default "info")
  -addr string
    	master's address
  -binlog-name string
    	startup binlog filename
  -binlog-pos int
    	startup binlog position (default 4)
  -log-file string
    	log file path
  -mode int
    	event read mode.
    	1: read packet with go-mysql;
    	2: read packet without go-mysql;
    	3: read binary data but do nothing
  -p username
    	password for username
  -server-id int
    	slave's server-id
  -u string
    	master's username
```

## Result

When exiting, the result will be output to the log as following

```log
[2019/08/12 15:44:19.269 +08:00] [INFO] [main.go:95] ["binlog-event-blackhole exit"] [event-count=35] [byte-count=2360] [duration=705.627314ms] [tps=49.601254522865595] ["throughput (byte/s)"=3344.541733541794]
```

| Item | Description |
|:------|:---- |
| event-count | The total events have received from the upstream master |
| byte-count | The total bytes have received from the upstream master |
| duration | The duration has be taken to fetch binlog events |
| tps | The events have received per second |
| throughput | The throughput of fetching binlog event data (bytes/second) |
