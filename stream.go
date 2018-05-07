package main

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/nareix/joy4/av"
	"github.com/nareix/joy4/av/avutil"
	"github.com/nareix/joy4/format"
	"github.com/nareix/joy4/format/rtmp"
	log "github.com/sirupsen/logrus"
)

var (
	ErrorNoURLs = errors.New("No URLs available for streaming...")

	// This is a workaround for a shortcoming of the joy4 library, where the end of a stream is
	// not detected cleanly
	ErrorExpectedTimeout = errors.New("Connection timed out")
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
	URLs      []string
	ReadFiles bool

	previousURLs []string
	opened       int
}

func (f *URLStreamFactory) NextURL() (string, error) {
	urls := f.URLs
	if f.ReadFiles {
		urls = f.loadURLs(urls)
	}
	res := f.nextURL(urls)
	if res == "" {
		return "", ErrorNoURLs
	}
	return res, nil
}

func (f *URLStreamFactory) loadURLs(filenames []string) (result []string) {
	for _, filename := range filenames {
		content, err := ioutil.ReadFile(filename)
		if err != nil {
			log.Errorf("Failed to read file with target URLs (%s): %v", filename, err)
			continue
		}
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if len(line) > 0 {
				result = append(result, line)
			}
		}
	}
	if len(result) == 0 {
		log.Errorf("No URLs loaded after reading %v file(s)... Sticking to old list of URLs!", len(filenames))
		result = f.previousURLs
	} else {
		log.Debugf("Loaded %v URL(s) from %v file(s)", len(result), len(filenames))
	}
	f.previousURLs = result
	return result
}

func (f *URLStreamFactory) nextURL(urls []string) string {
	if len(urls) == 0 {
		return ""
	}
	url := urls[f.opened%len(urls)]
	f.opened++
	return url
}

type HttpStreamFactory struct {
	URLStreamFactory
	ReceiveBuffer []byte // Shared between all streams, but the data is discarded anyways
}

func (f *HttpStreamFactory) OpenStream() (Stream, error) {
	url, err := f.NextURL()
	if err != nil {
		return nil, err
	}
	log.Debugln("Starting HTTP stream from", url)
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
	TimeoutDuration       time.Duration
}

func (f *MultimediaStreamFactory) OpenStream() (Stream, error) {
	url, err := f.NextURL()
	if err != nil {
		return nil, err
	}
	log.Debugln("Starting RTMP stream from", url)
	conn, err := avutil.Open(url)
	if err != nil {
		return nil, err
	}
	rtmpConn, ok := conn.(*rtmp.Conn)
	if !ok {
		return nil, fmt.Errorf("Opened multimedia connection (URL: %v) is not of type *rtmp.Conn, but %T", url, conn)
	}
	stream := &MultimediaStream{
		Conn:            rtmpConn,
		TimeoutDuration: f.TimeoutDuration,
	}
	stream.initialReceive(f.ExpectedInitialErrors)
	return stream, nil
}

type MultimediaStream struct {
	Conn            *rtmp.Conn
	TimeoutDuration time.Duration
}

func (f *MultimediaStream) Receive() (int, error) {
	pkt, err := f.readPacket()
	if err == ErrorExpectedTimeout {
		return 0, io.EOF
	}
	return len(pkt.Data), err
}

func (f *MultimediaStream) initialReceive(numErrors int) {
	for numErrors > 0 {
		_, err := f.readPacket()
		if err == io.EOF || err == nil {
			return
		} else {
			log.Debugln("Ignoring initial multimedia error:", err)
		}
		numErrors--
	}
}

func (f *MultimediaStream) readPacket() (av.Packet, error) {
	received := false
	timedOut := false
	if f.TimeoutDuration > 0 {
		time.AfterFunc(f.TimeoutDuration, func() {
			timedOut = true
			if !received {
				if err := f.Conn.Close(); err != nil {
					log.Errorf("Error closing a timeouted connection to %v: %v", f.Conn.NetConn().RemoteAddr, err)
				}
			}
		})
	}
	pkt, err := f.Conn.ReadPacket()
	received = true
	if timedOut && err != nil {
		// TODO find a better RTMP library and remove this workaround
		err = ErrorExpectedTimeout
		// err = fmt.Errorf("RTMP connection timed out: %v (Error: %v)", f.Conn.NetConn().RemoteAddr(), err)
	}
	return pkt, err
}
