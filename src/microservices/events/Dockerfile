FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod ./

# Download all dependencies
RUN go mod download

RUN apk --no-cache add musl-dev gcc
# Copy the source code
COPY . .

# Build the application
RUN CGO_ENABLED=1 GOOS=linux go build -tags musl -a -installsuffix cgo -o events .

# Use a smaller image for the final application
FROM alpine:latest  

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder
COPY --from=builder /app/events .

# Expose the application port
EXPOSE 8082

# Command to run the executable
CMD ["./events"]