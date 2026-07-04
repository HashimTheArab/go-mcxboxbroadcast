FROM golang:1.26.2-alpine AS build

RUN apk add --no-cache git ca-certificates && update-ca-certificates

WORKDIR /app/mcxboxbroadcast

# Copy dependency files first, this creates a layer that can be cached if the dependencies haven't changed
COPY go.mod go.sum ./

# Download dependencies (this layer will be cached if dependency files haven't changed)
RUN go mod download -x
ENV GOCACHE=/home/.cache/go-build

COPY . .

RUN --mount=type=cache,target="/home/.cache/go-build" CGO_ENABLED=0 go build -ldflags="-s -w" -o /app/mcxboxbroadcast_bin ./cmd/broadcaster

FROM alpine:3.22 AS runtime-base

RUN apk add --no-cache ca-certificates && update-ca-certificates

COPY --from=build /app/mcxboxbroadcast_bin /mcxboxbroadcast

FROM runtime-base AS pterodactyl

RUN addgroup -S container && adduser -S -G container -h /home/container container

ENV USER=container HOME=/home/container

WORKDIR /home/container

COPY deployments/pterodactyl/entrypoint.sh /entrypoint.sh
RUN chmod 755 /entrypoint.sh

USER container:container

CMD ["/bin/sh", "/entrypoint.sh"]

FROM runtime-base AS standalone

RUN addgroup -S app && adduser -S -G app -h /opt/app app

WORKDIR /opt/app/config

# chown must precede VOLUME: build steps that modify a path after it is
# declared a volume are discarded, leaving the mount root-owned at runtime.
RUN chown -R app:app /opt/app

VOLUME ["/opt/app/config"]

USER app:app

CMD ["/mcxboxbroadcast", "-config", "/opt/app/config/config.yml"]
