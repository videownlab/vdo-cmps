FROM golang:1.20-alpine3.19 AS builder

ARG GOPROXY
ARG https_proxy
ARG http_proxy

RUN set -eux; \
  apk add make git

# Download packages first so they can be cached.
COPY go.mod go.sum /opt/target/
RUN cd /opt/target/ && go mod download

COPY . /opt/target/

# Build the thing.
RUN cd /opt/target/ \
  && make

FROM alpine:3.19
WORKDIR /opt/vdo-cmps
COPY --from=builder /opt/target/vdo-cmps ./
COPY --from=builder /opt/target/static ./static