package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"path/filepath"
	"time"

	rtmp "github.com/antongulenko/rtmpclient"
	log "github.com/sirupsen/logrus"
)

const maxRtmpChannelNumber = 100

var ErrorNoURLs = errors.New("No URLs available for streaming...")

type RtmpStreamFactory struct {
	URLs            []string
	TimeoutDuration time.Duration
	opened          int
}

func (f *RtmpStreamFactory) nextURL() (string, error) {
	urls := f.URLs
	if len(urls) == 0 {
		return "", ErrorNoURLs
	}
	url := urls[f.opened%len(urls)]
	f.opened++
	return url, nil
}

func (f *RtmpStreamFactory) OpenStream() (*RtmpStream, error) {
	fullURL, err := f.nextURL()
	if err != nil {
		return nil, err
	}
	parsedURL, err := url.Parse(fullURL)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse URL (%v): %v", fullURL, err)
	}
	if parsedURL.Scheme != "rtmp" {
		return nil, fmt.Errorf("URL does not have 'rtmp' scheme: %v", fullURL)
	}
	urlPathPrefix, streamName := filepath.Split(parsedURL.Path)
	if urlPathPrefix == "" || streamName == "" {
		return nil, fmt.Errorf("URL path needs at least two components (have '%v' and '%v'): %v", urlPathPrefix, streamName, parsedURL.Path)
	}
	parsedURL.Path = urlPathPrefix

	// Establish connection
	dialURL := parsedURL.String()
	log.Debugln("Dialing RTMP URL:", dialURL)
	conn, err := rtmp.DialWithDialer(&net.Dialer{Timeout: f.TimeoutDuration}, dialURL, maxRtmpChannelNumber)
	if err != nil {
		return nil, err
	}
	err = conn.Connect()
	if err != nil {
		return nil, err
	}

	// Wait for the StreamCreatedEvent
	if err := f.startStream(conn, streamName); err != nil {
		return nil, err
	}
	return &RtmpStream{
		Conn:            conn,
		TimeoutDuration: f.TimeoutDuration,
	}, nil
}

func (f *RtmpStreamFactory) startStream(conn rtmp.ClientConn, streamName string) error {
	for done := false; !done; {
		select {
		case msg, ok := <-conn.Events():
			if !ok {
				return errors.New("Stream closed early")
			}
			switch ev := msg.Data.(type) {
			case *rtmp.StatusEvent:
				log.Debugf("Updated status while creating stream (%v): %v", conn.URL(), ev.Status)
			case *rtmp.CommandEvent:
				log.Debugf("Ignoring unexpected event while creating stream (%v): (%T) %v", conn.URL(), ev, ev)
			case *rtmp.StreamCreatedEvent:
				log.Debugln("Created RTMP stream with ID", ev.Stream.ID())
				return ev.Stream.Play(streamName, nil, nil, nil)
			default:
				return fmt.Errorf("Unexpected event while waiting for stream creation (%v) (type %T): %v", conn.URL(), msg.Data, msg.Data)
			}
		case <-time.After(f.TimeoutDuration):
			return fmt.Errorf("Timeout after %v waiting for data from %v", f.TimeoutDuration, conn.URL())
		}
	}
	panic("unreachable")
}

type RtmpStream struct {
	Conn            rtmp.ClientConn
	TimeoutDuration time.Duration
}

func (f *RtmpStream) Receive() (int, error) {
	for done := false; !done; {
		select {
		case msg, ok := <-f.Conn.Events():
			if !ok {
				return 0, errors.New("Stream closed early")
			}
			switch ev := msg.Data.(type) {
			case *rtmp.StatusEvent:
				log.Debugf("Updated status while waiting for data (%v): %v", f.Conn.URL(), ev.Status)
			case *rtmp.CommandEvent, *rtmp.StreamBegin, *rtmp.UnknownDataEvent, *rtmp.StreamIsRecorded, *rtmp.MetadataEvent:
				log.Debugf("Ignoring unexpected event while waiting for data (%v): (%T) %v", f.Conn.URL(), ev, ev)
			case *rtmp.AudioEvent:
				return int(ev.Message.Size), nil
			case *rtmp.VideoEvent:
				return int(ev.Message.Size), nil
			case *rtmp.StreamEOF:
				return 0, io.EOF
			default:
				return 0, fmt.Errorf("Unexpected event while waiting for data (%v) (type %T): %v", f.Conn.URL(), msg.Data, msg.Data)
			}
		case <-time.After(f.TimeoutDuration):
			return 0, fmt.Errorf("No stream started")
		}
	}
	panic("unreachable")
}

func (f *RtmpStream) Close() {
	if f == nil {
		return
	}
	if c := f.Conn; c != nil {
		c.Close()
	}
}
