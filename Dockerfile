# hub.docker.com/r/antongulenko/bitflow-collector
FROM golang:1.8.1

RUN mkdir -p /go/src/gitlab.tubit.tu-berlin.de/anton.gulenko/stream-statistics-client
COPY . /go/src/gitlab.tubit.tu-berlin.de/anton.gulenko/stream-statistics-client/
RUN go get -u gitlab.tubit.tu-berlin.de/anton.gulenko/stream-statistics-client
ENTRYPOINT ["stream-statistics-client"]

