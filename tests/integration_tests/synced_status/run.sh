#!/bin/bash

# [DISCRIPTION]:
#   This test is related to 
#   It will test the sync status request of cdc server in the following scenarios:(including both enable redo and disable redo)
#   1. The sync status request of cdc server when the upstream cluster is available
#      1.1 pdNow - lastSyncedTs > 5min, pdNow - checkpointTs < 5s
#      1.2 pdNow - lastSyncedTs < 5min
#   2. The sync status request of cdc server when the upstream pd is unavailable
#      2.1 resolvedTs - checkpointTs < 5s  
#   3. The sync status request of cdc server when the upstream tikv is unavailable
#      3.1 pdNow - lastSyncedTs > 5min, pdNow - checkpointTs > 5s, resolvedTs - checkpointTs < 5s  
#      3.2 pdNow - lastSyncedTs < 5min
#   4. The sync status request of cdc server when the downstream tidb is available
#      4.1 pdNow - lastSyncedTs > 5min, pdNow - checkpointTs < 5s
#      4.2 pdNow - lastSyncedTs < 5min
# [STEP]:
#   1. Create changefeed with synced-time-config = xx
#   2. insert data to upstream cluster, and do the related actions for each scenarios
#   3. do the query of synced status of cdc server
#   4. check the info and status of query

