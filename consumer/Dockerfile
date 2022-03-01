FROM golang AS builder

RUN apk update && apk add --no-cache git

WORKDIR /app

COPY . .

RUN go mod download


# Build the binary.
RUN  go build cmd/main

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