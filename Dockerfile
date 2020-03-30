# antongulenko/stream-statistics-client
FROM golang:1.14.1-alpine as build
RUN apk --no-cache add git gcc g++ musl-dev
WORKDIR /build
COPY . .
RUN find -name go.sum -delete
RUN sed -i $(find -name go.mod) -e '\_//.*gitignore$_d' -e '\_#.*gitignore$_d'
RUN go build -o /stream-statistics-client .

FROM alpine:3.11.5
RUN apk --no-cache add libstdc++
COPY --from=build /stream-statistics-client /
ENTRYPOINT ["/stream-statistics-client"]
