# How to use ansible to deploy dataflow engine

## Compile

make sure you have run `make` before deployment.

## Configure

Write your hostname or ip addresses of nodes in "hosts" file. For master servers, we need unique names for every node.

We config directory and ports in file `group_vars/all` for executors, masters, etcd, prometheus and node_exporter.

## Deploy

Execute `ansible-playbook -i hosts deploy.yml` to deploy. If the download of etcd, prometheus or node_exporter fails, just retry it.

## Start & Stop

Execute `ansible-playbook -i hosts start.yml` and `ansible-playbook -i hosts stop.yml` to start or stop cluster.

## Clean Environment

Execute `ansible-playbook -i hosts destroy.yml`

## Start Test

Enter `test/e2e` directory and change the tispace config file. Executor ` CONFIG=./tispace.json go test -run=TestSubmitTest`. The default timeout is 10 minutes. You can use `--timeout` to specify a timeout.