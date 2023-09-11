FROM golang:1.21
WORKDIR /d8x-candles
COPY . . 

#RUN apt update
RUN go mod download && go mod verify
RUN go build -o /usr/local/bin/app ./cmd/ws-server/main.go

CMD ["app"]