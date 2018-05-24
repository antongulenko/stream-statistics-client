package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

type SetUrlsRestApi struct {
	Col *StreamStatisticsCollector
}

func (api *SetUrlsRestApi) Register(pathPrefix string, router *mux.Router) {
	router.HandleFunc(pathPrefix+"/endpoints", api.handleEndpoints).Methods("GET", "POST", "PUT")
	router.HandleFunc(pathPrefix+"/streams", api.handleStreams).Methods("GET", "POST", "PUT")
}

func (c *SetUrlsRestApi) handleEndpoints(writer http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
	case "POST":
		lines := c.getRequestLines(writer, req)
		if len(lines) > 0 {
			c.Col.Factory.URLs = lines
		} else {
			return
		}
	case "PUT":
		lines := c.getRequestLines(writer, req)
		if len(lines) > 0 {
			c.Col.Factory.URLs = append(c.Col.Factory.URLs, lines...)
		} else {
			return
		}
	}
	urls := c.Col.Factory.URLs
	writer.Write([]byte(fmt.Sprintf("%v active endpoint(s):\n", len(urls))))
	for _, endpoint := range urls {
		writer.Write([]byte(endpoint + "\n"))
	}
}

func (c *SetUrlsRestApi) handleStreams(writer http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		writer.Write([]byte(fmt.Sprintf("Number of active streams: %v\n", len(c.Col.runningStreams))))
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
		previousNum := len(c.Col.runningStreams)
		c.Col.SetNumberOfStreams(num)
		writer.Write([]byte(fmt.Sprintf("Number of active streams set from %v to %v\n", previousNum, len(c.Col.runningStreams))))
	}
}

func (c *SetUrlsRestApi) getRequestLines(writer http.ResponseWriter, req *http.Request) []string {
	content, err := ioutil.ReadAll(req.Body)
	if err != nil {
		writer.Write([]byte(fmt.Sprintf("Failed to receive POST request body: %v\n", err)))
		writer.WriteHeader(http.StatusInternalServerError)
		return nil
	}
	lines := c.getStrippedLines(content)
	if len(lines) == 0 {
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write([]byte("Request body must define at least one non-empty URL\n"))
		return nil
	}
	return lines
}

func (c *SetUrlsRestApi) getStrippedLines(content []byte) []string {
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
