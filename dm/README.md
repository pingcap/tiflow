some useful integration test scripts

```shell
mysql -P4000 -h127.0.0.1 -uroot -e "drop database dmmeta"
mysql -P4000 -h127.0.0.1 -uroot -e "drop database test"  
```

```shell
bin/master --config=./sample/config/master.toml --master-addr 0.0.0.0:10240 --advertise-addr 127.0.0.1:10240 > /tmp/master.log &
bin/executor --config=./sample/config/executor.toml --join 127.0.0.1:10240 --worker-addr 0.0.0.0:10241 --advertise-addr 127.0.0.1:10241 > /tmp/executor.log &
```

```shell
bin/master-client --master-addr 127.0.0.1:10240 submit-job --job-type DM --job-config sample/config/dm-subtask.toml
```

```shell
mysql -P4000 -h127.0.0.1 -uroot -e "show databases"
```

```shell
killall master
killall executor
```