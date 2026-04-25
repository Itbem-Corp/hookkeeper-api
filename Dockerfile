FROM alpine:3.19

WORKDIR /app

# Install ca-certificates for HTTPS
RUN apk add --no-cache ca-certificates

# Copy binary
COPY hookkeeper-api /app/hookkeeper-api

# Expose port
EXPOSE 8080

# Run
CMD ["/app/hookkeeper-api"]
