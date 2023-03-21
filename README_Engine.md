# Tiflow engine

## Introduction

This repo implements a new engine of distributed task scheduler.

### Master

Master is set to process the requests from outside and to schedule and dispatch tasks.

### Executor

Executor is the worker process to run the tasks.

## Build

Simply run `make engine` to compile.

## Run Test

Use `make engine_unit_test` to run unit test and integrated test.

## Deploy Demonstration

### Single Master and Single Executor on Two Nodes

#### Start Master on Single Node

```[shell]
./bin/tiflow master --config=./deployments/engine/docker-compose/config/master.toml --addr 0.0.0.0:10240 --advertise-addr ${ip0}:10240 
```

Replace **ip0** with your advertising ip.

#### Start Executor

```[shell]
./bin/tiflow executor --config=./deployments/engine/docker-compose/config/executor.toml --join ${ip0}:10240 --addr 0.0.0.0:10241 --advertise-addr ${ip1}:10241
```

Replace **ip1** with your advertising executor ip.

### Three Master and One Executor on Three Nodes

Scaling out executor is quite simple. Right now we only support static config of masters, without scaling in and out dynamically.

In this case, we assume three node ips are ip0, ip1 and ip2. Replace them with real ip or hostname when operating.

#### Start Master on Node (ip0)

```[shell]
./bin/tiflow master --config=./deployments/engine/docker-compose/config/master.toml --addr 0.0.0.0:10240 --advertise-addr http://${ip0}:10240
```

Deploying masters for ip1 and ip2 are similar.

#### Start Executor on Node (ip0)

```[shell]
./bin/tiflow executor --config=./deployments/engine/docker-compose/config/executor.toml --join ${ip0}:10240 --addr 0.0.0.0:10241 --advertise-addr ${ip1}:10241
```

## Run engine in docker

This section shows how to start the cluster and run some tests by docker and docker-compose.

### Preparations

The following programs must be installed:

* docker
* docker-compose

Besides, make sure you have run the docker daemon. We recommend that you provide docker with at least 6+ cores and 8G+ memory. Of course, the more resources, the better.

Then, change directory to working directory:

```bash
cd ./deployments/engine/docker-compose
```

### Build

To build this repo, you can run `../run-engine.sh build` in working directory (or simply run `make engine_image` in root directory of tiflow project).

If you have build the binary on your local Linux, you can try `../run-engine.sh build-local`.

### Deploy

There are several configure files to use. The file name suggests the number of running server-master and executor nodes.   
For example, `1m1e.yaml` means this file contains one server-master and one executor.   
Use `../run-engine.sh deploy ./deployments/engine/docker-compose/1m1e.yaml` to deploy cluster.

### Destroy

Use `../run-engine.sh stop ./deployments/engine/docker-compose/1m1e.yaml` to destroy the cluster.

### Cleanup

sudo rm -rf /tmp/df/master

## Manager engine cluster by [helm](https://github.com/helm/helm) in local K8s
### Install tools
* [helm](https://helm.sh/docs/intro/install/)
* [kind](https://kind.sigs.k8s.io/)
* [kubectl](https://kubernetes.io/docs/tasks/tools/)

### Create a k8s cluster
```
$ kind create cluster
Creating cluster "kind" ...
 ✓ Ensuring node image (kindest/node:v1.24.0) 🖼 
 ✓ Preparing nodes 📦  
 ✓ Writing configuration 📜 
 ✓ Starting control-plane 🕹️ 
 ✓ Installing CNI 🔌 
 ✓ Installing StorageClass 💾 
Set kubectl context to "kind-kind"
You can now use your cluster with:

kubectl cluster-info --context kind-kind
Have a question, bug, or feature request? Let us know! https://kind.sigs.k8s.io/#community 🙂

$ kubectl cluster-info --context kind-kind
```

### Load engine image to k8s cluster
```
$ make engine_image
$ kind load docker-image dataflow:test
```

### Deploy engine cluster via helm
```
$ cd deployments/engine/helm
$ helm install test ./tiflow
NAME: test
LAST DEPLOYED: Fri Jul 22 18:52:02 2022
NAMESPACE: default
STATUS: deployed
REVISION: 1
TEST SUITE: None
$ helm list
NAME    NAMESPACE       REVISION        UPDATED                                 STATUS          CHART           APP VERSION
t1      default         1               2022-07-22 18:51:49.82093217 +0800 CST  deployed        tiflow-0.1.0    dev        
test    default         1               2022-07-22 18:52:02.96320918 +0800 CST  deployed        tiflow-0.1.0    dev   

$ kubectl get pods 
NAME                               READY   STATUS    RESTARTS        AGE
test-chaos-test-case-rvhx2         1/1     Running   0               6m58s
test-executor-0                    1/1     Running   0               6m58s
test-executor-1                    1/1     Running   0               6m58s
test-executor-2                    1/1     Running   0               6m58s
test-executor-3                    1/1     Running   0               6m58s
test-metastore-business-etcd-0     1/1     Running   0               6m58s
test-metastore-framework-mysql-0   1/1     Running   0               6m58s
test-server-master-0               1/1     Running   1 (4m49s ago)   6m58s
test-server-master-1               1/1     Running   1 (4m49s ago)   6m58s
test-server-master-2               1/1     Running   0               6m58s
```

## Contribute
