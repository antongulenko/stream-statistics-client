package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

type SetUrlsRestApi struct {
	Col *StreamStatisticsCollector
}

func (api *SetUrlsRestApi) Register(pathPrefix string, router *mux.Router) {
	router.HandleFunc(pathPrefix+"/endpoints", api.handleEndpoints).Methods("GET", "POST", "PUT")
	router.HandleFunc(pathPrefix+"/streams", api.handleStreams).Methods("GET", "POST", "PUT")
}

func (api *SetUrlsRestApi) handleEndpoints(writer http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
	case "POST":
		lines := api.getRequestLines(writer, req)
		if len(lines) > 0 {
			api.Col.Factory.hosts = nil
			api.Col.Factory.hostURLs = make(map[string][]*url.URL)
			api.appendEndpointURLs(lines, writer)
		} else {
			return
		}
	case "PUT":
		lines := api.getRequestLines(writer, req)
		if len(lines) > 0 {
			api.appendEndpointURLs(lines, writer)
		} else {
			return
		}
	}
}

func (api *SetUrlsRestApi) appendEndpointURLs(lines []string, writer http.ResponseWriter) {
	for _, entry := range lines {
		if host, urls, er := api.Col.Factory.ParseURLArgument(entry); er != nil {
			api.Col.Factory.hosts = append(api.Col.Factory.hosts, host)
			api.Col.Factory.hostURLs[host] = append(api.Col.Factory.hostURLs[host], urls...)
			writer.Write([]byte(fmt.Sprintf("For host %v successfully added following URLs as streaming endpoints: %v", host, urls)))
		} else {
			log.Errorf("Error handling streaming endpoint %v: %v", urls, er)
			writer.Write([]byte(fmt.Sprintf("Error handling streaming endpoint %v: %v", urls, er)))
		}

	}
}

func (api *SetUrlsRestApi) handleStreams(writer http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		writer.Write([]byte(fmt.Sprintf("Number of active streams: %v\n", len(api.Col.runningStreams))))
	case "POST", "PUT":
		numStr := req.FormValue("num")
		if numStr == "" {
			writer.WriteHeader(http.StatusBadRequest)
			writer.Write([]byte("Form or query parameter 'num' not defined\n"))
			return
		}
		num, err := strconv.Atoi(numStr)
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			writer.Write([]byte(fmt.Sprintf("Failed to parse value of form/query parameter 'num' ('%v': %v)\n", numStr, err)))
			return
		}
		previousNum := len(api.Col.runningStreams)
		api.Col.SetNumberOfStreams(num)
		writer.Write([]byte(fmt.Sprintf("Number of active streams set from %v to %v\n", previousNum, len(api.Col.runningStreams))))
	}
}

func (api *SetUrlsRestApi) getRequestLines(writer http.ResponseWriter, req *http.Request) []string {
	content, err := ioutil.ReadAll(req.Body)
	if err != nil {
		writer.Write([]byte(fmt.Sprintf("Failed to receive POST request body: %v\n", err)))
		writer.WriteHeader(http.StatusInternalServerError)
		return nil
	}
	lines := api.getStrippedLines(content)
	if len(lines) == 0 {
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write([]byte("Request body must define at least one non-empty URL\n"))
		return nil
	}
	return lines
}

func (api *SetUrlsRestApi) getStrippedLines(content []byte) []string {
	lines := strings.Split(string(content), "\n")
	cleanedLines := make([]string, 0, len(lines))
	for _, line := range lines {
		stripped := strings.TrimSpace(line)
		if stripped != "" {
			cleanedLines = append(cleanedLines, stripped)
		}
	}
	return cleanedLines
}
