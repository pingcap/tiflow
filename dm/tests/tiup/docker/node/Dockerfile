<<<<<<< HEAD
FROM golang:1.16
=======
FROM golang:1.27.3
>>>>>>> 4ff802a6ab (*: upgrade go version to 1.27.3 (#12904))

RUN apt-get update && \
    apt-get -y install \
        openssh-server \
        sudo vim \
        && \
    mkdir -p /var/run/sshd && \
    sed -i "s/UsePrivilegeSeparation.*/UsePrivilegeSeparation no/g" /etc/ssh/sshd_config && \
    sed -i "s/PermitRootLogin without-password/PermitRootLogin yes/g" /etc/ssh/sshd_config

ENV AUTHORIZED_KEYS **None**

ADD bashrc /root/.bashrc
ADD run.sh /run.sh
RUN chmod +x /*.sh

EXPOSE 22
CMD ["/run.sh"]
