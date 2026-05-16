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

FROM alpine:3.22

RUN apk add --no-cache ca-certificates && update-ca-certificates

WORKDIR /opt/app/config

COPY --from=build /app/mcxboxbroadcast_bin /mcxboxbroadcast

VOLUME ["/opt/app/config"]

CMD ["/mcxboxbroadcast", "-config", "/opt/app/config/config.yml"]
