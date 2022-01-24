# Specify the image architecture explicitly,
# otherwise it will not work correctly on other architectures.
FROM amd64/centos:centos7 as downloader

USER root
WORKDIR /root/download

COPY ./scripts/download-integration-test-binaries.sh .
# Download all binaries into bin dir.
RUN ./download-integration-test-binaries.sh master
RUN ls ./bin

# Download go into /usr/local dir.
ENV GOLANG_VERSION 1.16.4
ENV GOLANG_DOWNLOAD_URL https://dl.google.com/go/go$GOLANG_VERSION.linux-amd64.tar.gz
ENV GOLANG_DOWNLOAD_SHA256 7154e88f5a8047aad4b80ebace58a059e36e7e2e4eb3b383127a28c711b4ff59
RUN curl -fsSL "$GOLANG_DOWNLOAD_URL" -o golang.tar.gz \
	&& echo "$GOLANG_DOWNLOAD_SHA256  golang.tar.gz" | sha256sum -c - \
	&& tar -C /usr/local -xzf golang.tar.gz \
	&& rm golang.tar.gz

FROM amd64/centos:centos7

USER root
WORKDIR /root

# Installing dependencies.
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
RUN wget http://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum install -y epel-release-latest-7.noarch.rpm
RUN yum --enablerepo=epel install -y s3cmd
# Install mysql client.
RUN rpm -ivh https://repo.mysql.com/mysql57-community-release-el7-11.noarch.rpm
# See: https://support.cpanel.net/hc/en-us/articles/4419382481815?input_string=gpg+keys+problem+with+mysql+5.7
RUN rpm --import https://repo.mysql.com/RPM-GPG-KEY-mysql-2022
RUN yum install mysql-community-client.x86_64 -y

# Copy go form downloader.
COPY --from=downloader /usr/local/go /usr/local/go
ENV GOPATH /go
ENV GOROOT /usr/local/go
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH

WORKDIR /go/src/github.com/pingcap/tiflow
COPY . .

# Clean bin dir and build TiCDC.
# We always need to clean before we build, please don't adjust its order.
RUN make clean
RUN make integration_test_build kafka_consumer cdc
COPY --from=downloader /root/download/bin/* ./bin/
RUN make check_third_party_binary
