<<<<<<< HEAD
FROM golang:1.16
=======
FROM golang:1.27.3
>>>>>>> 4ff802a6ab (*: upgrade go version to 1.27.3 (#12904))

RUN apt-get -y -q update && \
    apt-get -y -q install software-properties-common && \
    apt-get install -qqy \
        default-mysql-client \
        psmisc

ADD bashrc /root/.bashrc
ADD init.sh /init.sh
RUN chmod +x /init.sh && \
    mkdir -p /root/.ssh && \
    echo "Host *\n    ServerAliveInterval 30\n    ServerAliveCountMax 3" >> /root/.ssh/config

CMD /init.sh
