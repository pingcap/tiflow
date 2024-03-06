ARG version
FROM mariadb:${version}

# https://github.com/docker-library/mysql/issues/1016#issuecomment-1857280082
RUN rm /etc/apt/sources.list.d/mariadb.list
RUN apt update && apt-get -y install keepalived net-tools

ADD init.sh /init.sh
ADD chk_mysql.sh /chk_mysql.sh

ARG conf
ADD $conf/keepalive.conf /etc/keepalived/keepalived.conf

RUN chmod +x /init.sh
RUN chmod +x /chk_mysql.sh

ENTRYPOINT ["/init.sh"]

# # 创建一个新用户并赋予相应的权限
# RUN useradd -ms /bin/bash mariadb
# RUN chown -R mariadb /var/lib/mysql

# # 切换到新用户
# USER mariadb

# RUN mysql_install_db --user=mariadb --ldata=/var/lib/mysql

CMD ["mysqld"]