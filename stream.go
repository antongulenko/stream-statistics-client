package main

import (
	"io"
	"net/http"

	"github.com/nareix/joy4/av"
	"github.com/nareix/joy4/av/avutil"
	"github.com/nareix/joy4/format"
	log "github.com/sirupsen/logrus"
)

func init() {
	format.RegisterAll()
}

type StreamFactory interface {
	OpenStream() (Stream, error)
}

type Stream interface {
	Receive() (int, error)
}

type URLStreamFactory struct {
	URLs   []string
	opened int
}

func (f *URLStreamFactory) NextURL() string {
	url := f.URLs[f.opened%len(f.URLs)]
	f.opened++
	return url
}

type HttpStreamFactory struct {
	URLStreamFactory
	ReceiveBuffer []byte // Shared between all streams, but the data is discarded anyways
}

func (f *HttpStreamFactory) OpenStream() (Stream, error) {
	url := f.NextURL()
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	return &HttpStream{
		Factory: f,
		Resp:    resp,
	}, nil
}

type HttpStream struct {
	Factory *HttpStreamFactory
	Resp    *http.Response
}

func (f *HttpStream) Receive() (int, error) {
	return f.Resp.Body.Read(f.Factory.ReceiveBuffer)
}

type MultimediaStreamFactory struct {
	URLStreamFactory
	ExpectedInitialErrors int
}

func (f *MultimediaStreamFactory) OpenStream() (Stream, error) {
	conn, err := avutil.Open(f.NextURL())
	if err != nil {
		return nil, err
	}
	stream := &MultimediaStream{
		Stream: conn,
	}
	stream.initialReceive(f.ExpectedInitialErrors)
	return stream, nil
}

type MultimediaStream struct {
	Stream av.DemuxCloser
}

func (f *MultimediaStream) Receive() (int, error) {
	pkt, err := f.Stream.ReadPacket()
	return len(pkt.Data), err
}

func (f *MultimediaStream) initialReceive(numErrors int) {
	for numErrors > 0 {
		_, err := f.Stream.ReadPacket()
		if err == io.EOF || err == nil {
			return
		} else {
			log.Debugln("Ignoring initial multimedia error:", err)
		}
		numErrors--
	}
}
