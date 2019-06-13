# antongulenko/stream-statistics-client
FROM golang:1.12-alpine as build
RUN apk --no-cache add git gcc g++ musl-dev
WORKDIR /build
COPY . .
RUN go build -o /stream-statistics-client ./...
ENTRYPOINT ["/stream-statistics-client"]

FROM alpine
RUN apk --no-cache add libstdc++
COPY --from=build /stream-statistics-client /
ENTRYPOINT ["/stream-statistics-client"]

