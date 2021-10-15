#!/bin/sh
if [ ! -d $GF_PROVISIONING_PATH/dashboards  ];then
    mkdir -p $GF_PROVISIONING_PATH/dashboards
fi

# DM
\cp -f /tmp/dm.json $GF_PROVISIONING_PATH/dashboards
sed -i 's/Test-Cluster-DM/'$DM_CLUSTER_NAME'-DM/g'  $GF_PROVISIONING_PATH/dashboards/dm.json

# Rules
if [ ! -d $PROM_CONFIG_PATH/rules  ];then
    mkdir -p $PROM_CONFIG_PATH/rules
fi

echo $META_TYPE
echo $META_INSTANCE
echo $META_VALUE
\cp -f /tmp/dm_worker.rules.yml $PROM_CONFIG_PATH/rules
sed -i 's/ENV_LABELS_ENV/'$DM_CLUSTER_NAME'/g' $PROM_CONFIG_PATH/rules/dm_worker.rules.yml

# Copy Persistent rules to override raw files
if [ ! -z $PROM_PERSISTENT_DIR ];
then
    if [ -d $PROM_PERSISTENT_DIR/latest-rules/${DM_VERSION##*/} ];then
        \cp -f $PROM_PERSISTENT_DIR/latest-rules/${DM_VERSION##*/}/dm_worker.rules.yml $PROM_CONFIG_PATH/rules
    fi
fi


# Datasources
if [ ! -z $GF_DATASOURCE_PATH ];
then

    if [ ! -z $GF_DM_PROMETHEUS_URL ];
    then
        sed -i 's,http://127.0.0.1:9090,'$GF_DM_PROMETHEUS_URL',g' /tmp/dm-cluster-datasource.yaml
    fi

    \cp -f /tmp/dm-cluster-datasource.yaml $GF_DATASOURCE_PATH/

fi

