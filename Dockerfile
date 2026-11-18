# Build the API binary.
FROM golang:1.25-alpine AS build
ENV GOTOOLCHAIN=auto
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/dpp ./cmd/api

# Small runtime image.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/dpp /app/dpp
COPY samples /app/samples
EXPOSE 8080
ENTRYPOINT ["/app/dpp"]
