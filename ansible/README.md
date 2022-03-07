# How to use ansible to deploy dataflow engine

- make sure you have ran `make` to build the binaries.
- write your hostname/ip of nodes in "hosts" file.
- declear your cluster name, and ports in "group_vars/all" file.
- execute `ansible-playbook -i hosts deploy.yml` to deploy.
- execute `ansible-playbook -i hosts start.yml` to start the cluster.

## todo

- support executor
- support monitor
- support demo and 100 cvs jobs
