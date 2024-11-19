FROM ghcr.io/pingcap-qe/ci/base:v2024.10.8-37-gee64991-go1.23

WORKDIR /go/src/github.com/pingcap/tiflow
COPY . .

COPY ./scripts/download-integration-test-binaries.sh .
# Download all binaries into bin dir.
RUN ./download-integration-test-binaries.sh master
RUN ls ./bin

# Clean bin dir and build TiCDC.
# We always need to clean before we build, please don't adjust its order.
RUN make clean
RUN make integration_test_build cdc
RUN make check_third_party_binary
