FROM busybox

ADD init.sh /usr/bin/init.sh
RUN chmod +x /usr/bin/init.sh

COPY dashboards/*.json /tmp/
COPY rules/*.rules.yml /tmp/
COPY datasources/*.yaml /tmp/

ENTRYPOINT ["/usr/bin/init.sh"]
CMD ["TIDB-Cluster", "/grafana-dashboard-definitions/tidb/", "false", "/etc/prometheus"]