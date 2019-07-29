FROM golang:1.12.6 AS builder
MAINTAINER Haystack <haystack@expedia.com>

RUN mkdir /work
COPY . /work/
WORKDIR /work
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o app .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /work/app .
EXPOSE 34002
CMD ["./app"]