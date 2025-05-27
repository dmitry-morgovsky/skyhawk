FROM golang:1.24 AS builder
WORKDIR /app
COPY statistics/go.mod statistics/go.sum ./
RUN go mod download

COPY statistics/ .

RUN CGO_ENABLED=0 go build -o /statistics ./cmd
FROM gcr.io/distroless/static:nonroot
COPY --from=builder /statistics /statistics
ENTRYPOINT ["/statistics"]
