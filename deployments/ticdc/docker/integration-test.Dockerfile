# Specify the image architecture explicitly,
# otherwise it will not work correctly on other architectures.
FROM amd64/centos:centos7 as downloader

ARG BRANCH
ENV BRANCH=$BRANCH

ARG COMMUNITY
ENV COMMUNITY=$COMMUNITY

ARG VERSION
ENV VERSION=$VERSION

ARG OS
ENV OS=$OS

ARG ARCH
ENV ARCH=$ARCH

USER root
WORKDIR /root/download

# Installing dependencies.
RUN yum install -y \
	wget
COPY ./scripts/download-integration-test-binaries.sh .
# Download all binaries into bin dir.
RUN ./download-integration-test-binaries.sh $BRANCH $COMMUNITY $VERSION $OS $ARCH
RUN ls ./bin

# Download go into /usr/local dir.
ENV GOLANG_VERSION 1.23.0
ENV GOLANG_DOWNLOAD_URL https://dl.google.com/go/go$GOLANG_VERSION.linux-amd64.tar.gz
RUN curl -fsSL "$GOLANG_DOWNLOAD_URL" -o golang.tar.gz \
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
	sudo \
	python3 \
    psmisc \
    procps
RUN wget http://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum install -y epel-release-latest-7.noarch.rpm
RUN yum --enablerepo=epel install -y s3cmd
# Install mysql client.
RUN rpm -ivh https://repo.mysql.com/mysql57-community-release-el7-11.noarch.rpm
# See: https://support.cpanel.net/hc/en-us/articles/4419382481815?input_string=gpg+keys+problem+with+mysql+5.7
RUN rpm --import https://repo.mysql.com/RPM-GPG-KEY-mysql-2022
RUN yum install mysql-community-client.x86_64 -y

# install java to run the schema regsitry for the avro case.
RUN yum install -y \
    java-1.8.0-openjdk \
    java-1.8.0-openjdk-devel

# Copy go form downloader.
COPY --from=downloader /usr/local/go /usr/local/go
ENV GOPATH /go
ENV GOROOT /usr/local/go
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH

WORKDIR /go/src/github.com/pingcap/tiflow
COPY . .

RUN --mount=type=cache,target=/root/.cache/go-build,target=/go/pkg/mod make integration_test_build cdc
COPY --from=downloader /root/download/bin/* ./bin/
RUN --mount=type=cache,target=/root/.cache/go-build,target=/go/pkg/mod make check_third_party_binary
