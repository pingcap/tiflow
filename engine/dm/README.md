## e2e test

### a dump-load-sync demo

start a 1-master-1-executor cluster, since now we can't require DMLoadWorker to
be started on the executor of DMDumpWorker.

```shell
cd sample && ./prepare.sh
rm -rf /tmp/df || true && rm -rf /tmp/dataflow || true
docker-compose -f ./1m1e.yaml up --force-recreate | tee /tmp/df.log
```

start MySQL listening on 0.0.0.0:3306 with user root and password "123456"
start TiDB listening on 0.0.0.0:4000 with user root and no password

in another shell

```shell
cd ./test/e2e && go test -count=1 -v -run=TestDMSubtask
```

the test should be passed.
