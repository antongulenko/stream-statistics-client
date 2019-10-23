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

	"github.com/antongulenko/golib"

	rtmp "github.com/antongulenko/rtmpclient"
	log "github.com/sirupsen/logrus"
)

const maxRtmpChannelNumber = 100

var ErrorNoURLs = errors.New("No URLs available for streaming...")

const urlTemplateRegexString = "{{(?P<min>[1-9][0-9]*) (?P<max>[1-9][0-9]*)}}" // {{123 456}}

var urlTemplateRegex = regexp.MustCompile(urlTemplateRegexString)

type RtmpEndpoint struct {
	url *url.URL

	// Meta info about the RTMP stream
	pixels uint
}

type RtmpHost struct {
	host      string
	endpoints []*RtmpEndpoint
}

func (h *RtmpHost) getRandomEndpoint() *RtmpEndpoint {
	numEndpoints := len(h.endpoints)
	randomIndex := rand.Intn(numEndpoints)
	return h.endpoints[randomIndex]
}

func (h *RtmpHost) addEndpoints(endpoints []*RtmpEndpoint) {
	h.endpoints = append(h.endpoints, endpoints...)
}

type RtmpStreamFactory struct {
	hosts       []*RtmpHost
	hostCounter int

	TimeoutDuration time.Duration
}

func (f *RtmpStreamFactory) printEndpoints(writer io.Writer) {
	fmt.Fprintln(writer, "Active endpoints:")
	for i, host := range f.hosts {
		fmt.Fprintf(writer, "\tHost %v: %v (%v endpoint(s))\n", i, host.host, len(host.endpoints))
		for j, endpoint := range host.endpoints {
			fmt.Fprintf(writer, "\t\tEndpoint %v (pixels: %v): %v\n", j, endpoint.pixels, endpoint.url)
		}
	}
}

func (f *RtmpStreamFactory) getHost(host string) *RtmpHost {
	for _, existingHost := range f.hosts {
		if existingHost.host == host {
			return existingHost
		}
	}
	newHost := &RtmpHost{
		host: host,
	}
	f.hosts = append(f.hosts, newHost)
	return newHost
}

func (f *RtmpStreamFactory) nextEndpoint() (*RtmpEndpoint, error) {
	for i := len(f.hosts); i >= 0; i-- {
		if nextHost, err := f.nextHost(); err != nil {
			return nil, ErrorNoURLs
		} else {
			if len(nextHost.endpoints) > 0 { // Success
				return nextHost.getRandomEndpoint(), nil
			}
		}
	}
	return nil, ErrorNoURLs
}

func (f *RtmpStreamFactory) nextHost() (*RtmpHost, error) {
	hosts := f.hosts
	if len(hosts) == 0 {
		return nil, ErrorNoURLs
	}
	nextHost := hosts[f.hostCounter%len(hosts)]
	f.hostCounter++
	return nextHost, nil
}

func (f *RtmpStreamFactory) OpenStream() (*RtmpStream, error) {
	rtmpEndpoint, err := f.nextEndpoint()
	if err != nil {
		return nil, err
	}
	conn, streamName, err := f.connect(rtmpEndpoint.url)
	if err != nil {
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
		Endpoint:        rtmpEndpoint,
	}, nil
}

func (f *RtmpStreamFactory) TestAllEndpointURLs() (string, error) {
	var counter, successCounter = 0, 0
	var multiErr = golib.MultiError{}
	for _, host := range f.hosts {
		counter += len(host.endpoints)
		for _, endpoint := range host.endpoints {
			conn, _, err := f.connect(endpoint.url)
			if err == nil {
				successCounter++
			} else {
				multiErr.Add(fmt.Errorf("Failed to connect to host %v via URL %v: %v",
					host, endpoint.url.String(), err))
			}
			if conn != nil {
				conn.Close()
			}
		}
	}
	summary := fmt.Sprintf("Endpoint connection test summary: Successfully connected to %v / %v endpoints.",
		successCounter, counter)
	err := multiErr.NilOrError()
	if err != nil {
		summary = fmt.Sprintf("%v\n Following errors occured:", summary)
	}
	return summary, err
}

func (f *RtmpStreamFactory) connect(target *url.URL) (rtmp.ClientConn, string, error) {
	if target.Scheme != "rtmp" {
		return nil, "", fmt.Errorf("URL does not have 'rtmp' scheme but '%v' scheme", target.Scheme)
	}
	urlPathPrefix, streamName := filepath.Split(target.Path)
	if urlPathPrefix == "" || streamName == "" {
		return nil, "", fmt.Errorf("URL path '%v' needs at least two components (have '%v' and '%v'): %v", target.Path, urlPathPrefix, streamName, target)
	}

	// Remove the trailing file name, because the named stream will be opened later
	urlCopy := *target
	urlCopy.Path = urlPathPrefix
	dialURL := urlCopy.String()

	// Establish connection
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

func (f *RtmpStreamFactory) ParseURLArgument(urlArg string) (string, []*RtmpEndpoint, error) {
	var unparsedURLs []string
	var regexInfo = fmt.Sprintf("Use regex that matches pattern '%v'", urlTemplateRegexString)

	match := urlTemplateRegex.FindStringSubmatch(urlArg)
	if match != nil { // URL is a template.
		log.Infof("Processing template URL %v with regex matching. (Used regex: '%v')", urlArg, urlTemplateRegexString)

		min, err := strconv.Atoi(match[1])
		if err != nil {
			return "", nil, fmt.Errorf("Failed to parse minimal value from url %v. %v: %v", urlArg, regexInfo, err)
		}
		max, err := strconv.Atoi(match[2])
		if err != nil {
			return "", nil, fmt.Errorf("Failed to parse maximal value from url %v. %v: %v", urlArg, regexInfo, err)
		}
		if min > max {
			return "", nil, fmt.Errorf("Minimal value cannot be greater than maximal value in url %v. %v.", urlArg, regexInfo)
		}
		if urls, err := f.generateURLs(urlArg, match[0], min, max); err == nil {
			unparsedURLs = append(unparsedURLs, urls...)
		} else {
			return "", nil, fmt.Errorf("URL generation based on template URL %v failed. %v: %v", urlArg, regexInfo, err)
		}
	} else { // No matching regex expression found in url argument. URL is not a template. Returning it as it is.
		log.Infof("URL is not a template. Parsing URL %v without regex matching. (Used regex: '%v')", urlArg, urlTemplateRegexString)
		unparsedURLs = append(unparsedURLs, urlArg)
	}

	var endpoints []*RtmpEndpoint
	var multiErr = golib.MultiError{}
	for _, unparsedURL := range unparsedURLs {
		parsedURL, err := url.Parse(unparsedURL)
		if err != nil {
			multiErr.Add(fmt.Errorf("Failed to parse URL %v: %v", unparsedURL, err))
		} else {
			log.Debugf("Parsed URL %v from URL template %v.", parsedURL.String(), urlArg)

			// Take the query parameter pixels=XXX and parse it to an integer, to obtain the resolution as meta information (if available).
			// Afterwards, delete the query parameter to fix the URL.
			pixelsStr := parsedURL.Query().Get("pixels")
			pixels := 0
			if pixelsStr != "" {
				if parsedPixels, err := strconv.Atoi(pixelsStr); err != nil {
					log.Errorf("URL %v contains 'pixels' query parameter, which could not be parsed to integer: %v", parsedURL, err)
				} else {
					pixels = parsedPixels
				}
			}
			modifiedQuery := parsedURL.Query()
			modifiedQuery.Del("pixels")
			parsedURL.RawQuery = modifiedQuery.Encode()

			endpoints = append(endpoints, &RtmpEndpoint{
				url:    parsedURL,
				pixels: uint(pixels),
			})
		}
	}

	var host string
	err := multiErr.NilOrError()
	if len(endpoints) > 0 {
		host = endpoints[0].url.Host
	} else {
		return "", nil, fmt.Errorf("Failed to parse streaming endpoint urls from template %v: %v", urlArg, err)
	}
	return host, endpoints, err
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
	Endpoint        *RtmpEndpoint
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
