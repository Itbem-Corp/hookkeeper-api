FROM golang:1.22-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/hookkeeper-api .

FROM alpine:3.19

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY --from=builder /out/hookkeeper-api /app/hookkeeper-api

EXPOSE 8080

CMD ["/app/hookkeeper-api"]
