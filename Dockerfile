# hub.docker.com/r/antongulenko/bitflow-collector
FROM golang:1.8.1

WORKDIR /go/src/gitlab.tubit.tu-berlin.de/anton.gulenko/stream-statistics-client
COPY . .
RUN go install gitlab.tubit.tu-berlin.de/anton.gulenko/stream-statistics-client
ENTRYPOINT ["stream-statistics-client"]

