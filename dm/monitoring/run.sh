\cp -f ../dm/dm-ansible/scripts/DM-Monitor-Standard.json ./dashboards/
\cp -f ../dm/dm-ansible/scripts/DM-Monitor-Professional.json ./dashboards/
\cp -f ../dm/dm-ansible/conf/dm_worker.rules.yml ./rules/

go run dashboards/dashboard.go
docker build . -t $1
