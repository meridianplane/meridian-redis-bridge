FROM golang:1.24-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /meridian ./cmd/meridian

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /meridian /usr/local/bin/meridian
ENTRYPOINT ["meridian"]
CMD ["-config", "/etc/meridian/config.json"]
