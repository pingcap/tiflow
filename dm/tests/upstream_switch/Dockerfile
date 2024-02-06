ARG version
FROM mysql:${version}-debian

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys B7B3B788A8D3785C
RUN apt-get update && apt-get -y install keepalived net-tools

ADD init.sh /init.sh
ADD chk_mysql.sh /chk_mysql.sh

ARG conf
ADD $conf/keepalive.conf /etc/keepalived/keepalived.conf

RUN chmod +x /init.sh
RUN chmod +x /chk_mysql.sh

ENTRYPOINT ["/init.sh"]

CMD ["mysqld"]
