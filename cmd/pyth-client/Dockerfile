FROM golang:1.21
WORKDIR /d8x-candles
COPY . . 
#RUN apt update
RUN go mod download && go mod verify
RUN go build -o /usr/local/bin/app ./cmd/pyth-client/main.go

FROM debian:bookworm-slim
COPY --from=0 /usr/local/bin/app /usr/local/bin/app
CMD ["app"]