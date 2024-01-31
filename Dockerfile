FROM golang:1.21.6-bullseye as builder
WORKDIR $GOPATH/src/github.com/thanos-io/thanosbench
# Replaced ADD with COPY as add is generally to download content form link or tar files
# while COPY supports the basic copying of local files into the container.
# https://docs.docker.com/develop/develop-images/dockerfile_best-practices/#add-or-copy
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN git update-index --refresh; make build
# -----------------------------------------------------------------------------
FROM quay.io/prometheus/busybox:latest
LABEL maintainer="The Thanos Authors"
COPY --from=builder /go/src/github.com/thanos-io/thanosbench/thanosbench /bin/thanosbench
ENTRYPOINT [ "/bin/thanosbench" ]