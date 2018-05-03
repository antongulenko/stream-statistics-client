# hub.docker.com/r/antongulenko/bitflow-collector
FROM golang:1.8.1

#RUN mkdir -p /go/src/gitlab.tubit.tu-berlin.de/anton.gulenko/stream-statistics-client
WORKDIR /go/src/gitlab.tubit.tu-berlin.de/anton.gulenko/stream-statistics-client
#COPY . /go/src/gitlab.tubit.tu-berlin.de/anton.gulenko/stream-statistics-client/
COPY . .
RUN git remote set-url origin 
RUN go get -f -u gitlab.tubit.tu-berlin.de/anton.gulenko/stream-statistics-client
ENTRYPOINT ["stream-statistics-client"]

