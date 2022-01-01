FROM canal/osbase:v2

# Solve the problem that centos 6.0 cannot yum install.
RUN sed -i "s|enabled=1|enabled=0|g" /etc/yum/pluginconf.d/fastestmirror.conf
RUN mv /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo.backup
RUN curl -o /etc/yum.repos.d/CentOS-Base.repo https://www.xmpan.com/Centos-6-Vault-Aliyun.repo
RUN yum clean all
RUN yum makecache

# Install mysql client.
RUN wget https://dev.mysql.com/get/mysql57-community-release-el6-9.noarch.rpm
RUN rpm -ivh mysql57-community-release-el6-9.noarch.rpm
RUN yum install mysql-community-client.x86_64 -y

WORKDIR /root
RUN wget https://github.com/alibaba/canal/releases/download/canal-1.1.5/canal.adapter-1.1.5.tar.gz
RUN tar xf canal.adapter-1.1.5.tar.gz
COPY . .
RUN chmod 777 start.sh

CMD ["/bin/sh", "./start.sh"]

