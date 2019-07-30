package main

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	rtmp "github.com/antongulenko/rtmpclient"
	log "github.com/sirupsen/logrus"
)

const maxRtmpChannelNumber = 100

var ErrorNoURLs = errors.New("No URLs available for streaming...")

type RtmpStreamFactory struct {
	hosts                []string
	hostURLs             map[string][]*url.URL
	TimeoutDuration      time.Duration
	hostSelectionCounter int
	opened               int
}

func (f *RtmpStreamFactory) nextURL() (*url.URL, error) {
	for i := len(f.hosts); i >= 0; i-- {
		if nextHost, er := f.nextHost(); er != nil {
			return nil, ErrorNoURLs
		} else {
			if len(f.hostURLs[nextHost]) > 0 { // Success
				return f.hostURLs[nextHost][rand.Intn(len(f.hostURLs[nextHost]))], nil // Pick random URL of that host
			}
		}
	}
	return nil, ErrorNoURLs
}

func (f *RtmpStreamFactory) nextHost() (string, error) {
	hosts := f.hosts
	if len(hosts) == 0 {
		return "", ErrorNoURLs
	}
	nextHost := hosts[f.hostSelectionCounter%len(hosts)]
	f.hostSelectionCounter++
	return nextHost, nil
}

func (f *RtmpStreamFactory) OpenStream() (*RtmpStream, error) {
	parsedURL, err := f.nextURL()
	if err != nil {
		return nil, err
	}
	conn, streamName, err := f.connect(parsedURL)
	if err == nil {
		return nil, err
	}
	// Wait for the StreamCreatedEvent
	if err := f.startStream(conn, streamName); err != nil {
		conn.Close()
		return nil, err
	}
	return &RtmpStream{
		Conn:            conn,
		TimeoutDuration: f.TimeoutDuration,
	}, nil
}

func (f *RtmpStreamFactory) TestAllEndpointURLs() string {
	var counter, successCounter int
	var failedEntries []string
	for _, host := range f.hosts {
		counter += counter + len(f.hostURLs[host])
		for _, parsedUrl:= range f.hostURLs[host] {
			_, _, err := f.connect(parsedUrl)
			if err == nil {
				failedEntries = append(failedEntries, fmt.Sprintf("Failed to connect to host %v via URL %v: %v", host, parsedUrl.String(), err))
			} else {
				successCounter++
			}
		}
	}
	summary := fmt.Sprintf("Endpoint connection test summary:\n" +
		"Successfully connected to %v / %v endpoints.\n", counter, successCounter)
	if len(failedEntries) > 0 {
		summary = fmt.Sprintf("%v\n Following errors occured:", summary)
		for _, entry := range failedEntries {
			summary = fmt.Sprintf("%v\n%v", summary, entry)
		}
	}
	return summary
}

func (f *RtmpStreamFactory) connect(url *url.URL) (rtmp.ClientConn, string, error) {
	if url.Scheme != "rtmp" {
		return nil, "", fmt.Errorf("URL does not have 'rtmp' scheme but '%v' scheme", url.Scheme)
	}
	urlPathPrefix, streamName := filepath.Split(url.Path)
	if urlPathPrefix == "" || streamName == "" {
		return nil, "", fmt.Errorf("URL path needs at least two components (have '%v' and '%v'): %v", urlPathPrefix, streamName, url.Path)
	}
	url.Path = urlPathPrefix

	// Establish connection
	dialURL := url.String()
	log.Debugln("Dialing RTMP URL:", dialURL)
	conn, err := rtmp.DialWithDialer(&net.Dialer{Timeout: f.TimeoutDuration}, dialURL, maxRtmpChannelNumber)
	if err != nil {
		return nil, "", err
	}
	err = conn.Connect()
	if err != nil {
		return nil, "", err
	}
	return conn, streamName, nil
}

func (f *RtmpStreamFactory) startStream(conn rtmp.ClientConn, streamName string) error {
	for {
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
}

func (f *RtmpStreamFactory) ParseURLArgument(urlArg string) (string, []*url.URL, error) {
	var host string
	var unparsedURLs []string
	var urls []*url.URL
	var r = regexp.MustCompile(`{{(?P<min>[1-9][0-9]*) (?P<max>[1-9][0-9]*)}}`) // {{123 456}}

	var regexPattern = "'{{(?P<min>[1-9][0-9]*) (?P<max>[1-9][0-9]*)}}'"
	var regexInfo = fmt.Sprintf("Use regex that matches patter %v", regexPattern)

	if r.MatchString(urlArg) { // URL is a template.
		log.Infof("Processing template URL %v with regex matching. (Used regex: '%v')", urlArg, regexPattern)
		match := r.FindStringSubmatch(urlArg)
		min, er := strconv.Atoi(match[1])
		if er != nil {
			return "", nil, fmt.Errorf("Failed to parse minimal value from url %v. %v: %v", urlArg, regexInfo, er)
		}
		max, er := strconv.Atoi(match[2])
		if er != nil {
			return "", nil, fmt.Errorf("Failed to parse maximal value from url %v. %v: %v", urlArg, regexInfo, er)
		}
		if min > max {
			return "", nil, fmt.Errorf("Minimal value cannot be greater than maximal value in url %v. %v.", urlArg, regexInfo)
		}
		if urls, er := f.generateURLs(urlArg, match[0], min, max); er == nil {
			unparsedURLs = append(unparsedURLs, urls...)
		} else {
			return "", nil, fmt.Errorf("URL generation based on template URL %v failed. %v: %v", urlArg, regexInfo, er)
		}
	} else { // No matching regex expression found in url argument. URL is not a template. Returning it as it is.
		log.Infof("URL is not a template. Parsing URL %v without regex matching. (Used regex: '%v')", urlArg, regexPattern)
		unparsedURLs = append(unparsedURLs, urlArg)
	}

	var errorMsg string
	for _, unparsedURL := range unparsedURLs {
		parsedURL, err := url.Parse(unparsedURL)
		if err != nil {
			if len(errorMsg) == 0 {
				errorMsg = fmt.Sprintf("%v: %v\n", unparsedURLs, err)
			} else {
				errorMsg = fmt.Sprintf("%v, %v: %v\n", errorMsg, unparsedURLs, err)
			}
		} else {
			urls = append(urls, parsedURL)
		}
	}
	if len(urls) > 0 {
		host = urls[0].Host
	} else {
		return "", nil, fmt.Errorf("Failed to parse streaming endpoint urls from template %v", urlArg)
	}
	if len(errorMsg) != 0 {
		return host, urls, nil
	} else {
		return "", nil, fmt.Errorf("Following urls that were generated from template url %v could not be parsed: %v", errorMsg)
	}
}

func (f *RtmpStreamFactory) generateURLs(urlArg string, toReplace string, min int, max int) ([]string, error) {
	var urls []string

	if strings.Contains(urlArg, toReplace) {
		for i := min; i <= max; i++ {
			unparsedURL := strings.Replace(urlArg, toReplace, strconv.Itoa(i), 1)
			urls = append(urls, unparsedURL)
		}
	} else {
		return nil, fmt.Errorf("URL generation failed. Template URL %v does not contain substring %v to replace.", urlArg, toReplace)
	}
	return urls, nil
}

type RtmpStream struct {
	Conn            rtmp.ClientConn
	TimeoutDuration time.Duration
}

func (f *RtmpStream) Receive() (int, error) {
	for {
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
}

func (f *RtmpStream) Close() {
	if f == nil {
		return
	}
	if c := f.Conn; c != nil {
		c.Close()
	}
}
