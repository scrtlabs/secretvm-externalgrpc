# ==========================================
# Stage 1: Build the Go Binary
# ==========================================
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o secretvm-externalgrpc .

# ==========================================
# Stage 2: Create the Final Runtime Image
# ==========================================
FROM alpine:3.19

WORKDIR /root/

RUN apk add --no-cache nodejs npm 

RUN npm install -g secretvm-cli

COPY --from=builder /app/secretvm-externalgrpc .

EXPOSE 8888

CMD ["./secretvm-externalgrpc"]
