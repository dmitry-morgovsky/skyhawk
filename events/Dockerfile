FROM golang:1.24 AS builder
WORKDIR /app
COPY events/go.mod events/go.sum ./
RUN go mod download

COPY events/ .

RUN CGO_ENABLED=0 go build -o /events ./cmd

#FROM gcr.io/distroless/static:nonroot
FROM alpine
COPY --from=builder /events /events
RUN chmod +x /events

ENTRYPOINT ["/events"]
