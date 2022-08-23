## Preparations

The following programs must be installed:

* [docker](https://docs.docker.com/get-docker/)
* [docker-compose](https://docs.docker.com/compose/install/)

Besides, make sure you have run the docker daemon. We recommend that you provide docker with at least 6+ cores and 8G+ memory. Of course, the more resources, the better.

## Setup environment 
`NOTICE`: some e2e tests like `e2e_node_chaos_test` rely on the failover mechanism of k8s, so it can't run locally.
```
// Now we are in e2e dir
1. Go to the tiflow/deployments dir
cd ../../../deployments/engine/docker-compose/

2. Start tiflow cluster
../run_engine build (ignore this one if you have builded the image)
../run_engine deploy ./deployments/engine/docker-compose/1m1e.yaml

3. Start dm upstream and downstream
../run_engine deploy ./deployments/engine/docker-compose/dm_databases.yaml
```

## Run e2e test
```
// Now we are in e2e dir
go test -v -race -run="TestDMJob"
```

