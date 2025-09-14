FROM golang:1.22-alpine
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -o server
EXPOSE 8080
CMD ["/app/server"]
