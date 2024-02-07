ARG version
FROM mysql:${version}-debian

# https://github.com/docker-library/mysql/issues/1016#issuecomment-1857280082
RUN rm /etc/apt/sources.list.d/mysql.list
RUN apt-get update && apt-get -y install keepalived net-tools

ADD init.sh /init.sh
ADD chk_mysql.sh /chk_mysql.sh

ARG conf
ADD $conf/keepalive.conf /etc/keepalived/keepalived.conf

RUN chmod +x /init.sh
RUN chmod +x /chk_mysql.sh

ENTRYPOINT ["/init.sh"]

CMD ["mysqld"]
