FROM centos:centos7

USER root
WORKDIR /root

RUN yum install -y \
	git \
	bash-completion \
	wget \
    which \
	gcc \
	make \
    curl \
    tar \
    musl-dev \
    psmisc

RUN wget -i -c http://dev.mysql.com/get/mysql57-community-release-el7-10.noarch.rpm
RUN yum install -y mysql57-community-release-el7-10.noarch.rpm
RUN yum install -y mysql-server

ENV GOLANG_VERSION 1.16.4
ENV GOLANG_DOWNLOAD_URL https://dl.google.com/go/go$GOLANG_VERSION.linux-amd64.tar.gz
ENV GOLANG_DOWNLOAD_SHA256 7154e88f5a8047aad4b80ebace58a059e36e7e2e4eb3b383127a28c711b4ff59

RUN curl -fsSL "$GOLANG_DOWNLOAD_URL" -o golang.tar.gz \
	&& echo "$GOLANG_DOWNLOAD_SHA256  golang.tar.gz" | sha256sum -c - \
	&& tar -C /usr/local -xzf golang.tar.gz \
	&& rm golang.tar.gz

ENV GOPATH /go
ENV GOROOT /usr/local/go
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH

WORKDIR /go/src/github.com/pingcap/ticdc
COPY . .
RUN cd ./scripts && ./prepare-integration-test-binaries.sh master
