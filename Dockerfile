# antongulenko/stream-statistics-client
FROM golang:1.11-alpine as build
ENV GO111MODULE=on
RUN apk --no-cache add git gcc g++ musl-dev
WORKDIR /build
COPY . .
RUN go build -o /stream-statistics-client ./...
ENTRYPOINT ["/stream-statistics-client"]

FROM alpine
RUN apk --no-cache add libstdc++
COPY --from=build /stream-statistics-client /
ENTRYPOINT ["/stream-statistics-client"]

