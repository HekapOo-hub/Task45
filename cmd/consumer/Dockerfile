FROM golang AS builder


WORKDIR /app

COPY . .

RUN go mod download


# Build the binary.
RUN go build consumer_main.go

#####################################
#   STEP 2 build a small image      #
#####################################
FROM alpine:latest

RUN apk --no-cache add ca-certificates
RUN apk add --no-cache libc6-compat

# Copy our static executable.
COPY --from=builder /app/main /app/main

# Run the hello binary.
ENTRYPOINT ["/app/main"]