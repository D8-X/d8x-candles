FROM golang:1.21

ARG BUILD_VERSION

WORKDIR /d8x-candles
COPY . . 

#RUN apt update
RUN go mod download && go mod verify
RUN go build -ldflags "-X 'main.VERSION=${BUILD_VERSION}'" -o /usr/local/bin/app ./cmd/ws-server/main.go

FROM debian:bookworm-slim
COPY --from=0 /usr/local/bin/app /usr/local/bin/app
RUN apt-get update && apt-get install -y ca-certificates
CMD ["app"]