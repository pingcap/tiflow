# How to build DM grafana dashboard

```bash
cp -f ../dm/dm-ansible/scripts/DM-Monitor-Standard.json ./dashboards/
cp -f ../dm/dm-ansible/scripts/DM-Monitor-Professional.json ./dashboards/
mkdir -p monitoring/rules
cp -f dm/dm-ansible/conf/dm_worker.rules.yml monitoring/rules/
cd monitoring && go run dashboards/dashboard.go
```
