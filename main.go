package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/antongulenko/golib"
	"github.com/bitflow-stream/go-bitflow/bitflow"
	"github.com/bitflow-stream/go-bitflow/cmd"
	log "github.com/sirupsen/logrus"
)

const noUrlsSleepDuration = 5 * time.Second

func main() {
	os.Exit(do_main())
}

func do_main() int {
	var delaySampler DistributionSampler
	parallelStreams := flag.Int("n", 1, "Number of parallel streams to start immediately")
	flag.Var(&delaySampler, "restartDelayDistribution", "Define an random distribution for the time before starting a stream."+
		" This is applied, when streams are initially started and when a stream ends (with or without error). Definition format: "+
		"<distribution type>:<comma separated list of duration parameters>. Supported distribution types  (with required parameters): "+
		"'const:<value>', 'equal:<min_value>,<max_value>', 'norm:<mean>,<std_dev>'. Examples: 'const:500ms', 'const:5s', 'norm:100ms,30ms', 'equal:0ms,1s'.")
	sinkInterval := flag.Duration("si", 1000*time.Millisecond, "Interval in which to send out stream statistics")
	timeout := flag.Duration("timeout", 5*time.Second, "Timeout for RTMP streams")
	testEndpoints := flag.Bool("test", false, "Test initial endpoints by trying to connect to each and log the summarized results before "+
		"the regular streaming is started.")
	if delaySampler.distribution == nil {
		delaySampler = DistributionSampler{distribution: &ConstDistribution{0 * time.Millisecond}}
		log.Infof("No restart delay distribution defined. Using: %v", delaySampler.String())
	}

	rand.Seed(time.Now().UTC().UnixNano())
	factory := &RtmpStreamFactory{
		TimeoutDuration: *timeout,
	}
	helper := cmd.CmdDataCollector{DefaultOutput: "csv://-"}
	helper.RegisterFlags()
	_, args := cmd.ParseFlags()
	defer golib.ProfileCpu()()
	if len(args) > 0 {
		for _, urlTemplate := range args {
			if host, endpoints, err := factory.ParseURLArgument(urlTemplate); err == nil {
				host := factory.getHost(host)
				host.addEndpoints(endpoints)
			} else {
				log.Errorf("Error handling streaming endpoint %v: %v", urlTemplate, err)
			}
		}
		if *testEndpoints {
			summary, err := factory.TestAllEndpointURLs()
			log.Info(summary)
			if err != nil {
				log.Errorf("%v", err)
			}
		}
	} else {
		log.Info("No streaming endpoints defined. Cannot request streams. Use /api/endpoints to add streaming endpoints.")
	}

	stats := &StreamStatisticsCollector{
		InitialStreams:     *parallelStreams,
		Factory:            factory,
		DelaySampler:       delaySampler,
		SampleSinkInterval: *sinkInterval,
	}
	helper.RestApis = append(helper.RestApis, &SetUrlsRestApi{Col: stats})

	pipe, err := helper.BuildPipeline(stats)
	golib.Checkerr(err)
	return pipe.StartAndWait()
}

type StreamStatisticsCollector struct {
	bitflow.AbstractSampleSource

	InitialStreams     int
	Factory            *RtmpStreamFactory
	DelaySampler       DistributionSampler
	SampleSinkInterval time.Duration
	RestApiEndpoint    string

	wg             *sync.WaitGroup
	runningStreams []*RunningStream
	streamsLock    sync.Mutex
	stopper        golib.StopChan

	// Stream statistics
	statisticsTime       time.Time
	openConnections      TwoWayCounter
	receivingConnections TwoWayCounter
	opened               IncrementedCounter
	closed               IncrementedCounter
	errors               IncrementedCounter
	bytes                IncrementedCounter
	packets              IncrementedCounter
	packetDelay          AveragingCounter
	pixels               TwoWayCounter
}

func (c *StreamStatisticsCollector) String() string {
	return fmt.Sprintf("Measure %v stream(s) from %T", len(c.runningStreams), c.Factory)
}

func (c *StreamStatisticsCollector) Start(wg *sync.WaitGroup) golib.StopChan {
	c.wg = wg
	c.stopper = golib.NewStopChan()
	wg.Add(1)
	go c.sinkSamples(wg)
	c.SetNumberOfStreams(c.InitialStreams)
	return c.stopper
}

func (c *StreamStatisticsCollector) SetNumberOfStreams(num int) {
	c.streamsLock.Lock()
	defer c.streamsLock.Unlock()
	if num < 0 {
		num = 0
	}
	if len(c.runningStreams) > num {
		// Close excess the streams
		toClose := c.runningStreams[num:]
		c.runningStreams = c.runningStreams[:num]
		log.Printf("Closing %v stream(s), new number of streams: %v", len(toClose), len(c.runningStreams))
		for _, stream := range toClose {
			stream.stop()
		}
	} else if len(c.runningStreams) < num {
		// Spawn missing streams if not stopped yet
		if c.stopper.Stopped() {
			return
		}
		missing := num - len(c.runningStreams)
		log.Printf("Starting %v new stream(s), new number of streams: %v", missing, len(c.runningStreams)+missing)
		for i := 0; i < missing; i++ {
			newStream := &RunningStream{col: c, stopper: golib.NewStopChan()}
			c.runningStreams = append(c.runningStreams, newStream)
			newStream.start()
		}
	}
}

func (c *StreamStatisticsCollector) Close() {
	c.stopper.Stop()
	c.SetNumberOfStreams(0)
}

func (c *StreamStatisticsCollector) sinkSamples(wg *sync.WaitGroup) {
	defer wg.Done()
	defer c.CloseSinkParallel(wg)
	c.statisticsTime = time.Now()
	for c.stopper.WaitTimeout(c.SampleSinkInterval) {
		now := time.Now()
		previousTime := c.statisticsTime
		c.statisticsTime = now
		timeDiff := now.Sub(previousTime)
		opened, openedDiff := c.opened.ComputeDiff(timeDiff)
		closed, closedDiff := c.closed.ComputeDiff(timeDiff)
		errors, errorsDiff := c.errors.ComputeDiff(timeDiff)
		bytes, bytesDiff := c.bytes.ComputeDiff(timeDiff)
		packets, packetsDiff := c.packets.ComputeDiff(timeDiff)
		packetDelay := c.packetDelay.ComputeAvg()
		pixels := c.pixels.Get()
		receivingConnections := c.receivingConnections.Get()
		values := []bitflow.Value{
			// Meta values
			bitflow.Value(len(c.runningStreams)),
			c.openConnections.Get(),
			receivingConnections,
			// Absolute values
			opened, closed, errors, bytes, packets,
			// Values per second
			openedDiff, closedDiff, errorsDiff, bytesDiff, packetsDiff,
			// Average values
			packetDelay,
			// Pixels and values per pixel
			pixels, bytesDiff / pixels, packetsDiff / pixels,
			// Values per running connection
			bytesDiff / receivingConnections, packetsDiff / receivingConnections,
		}
		fields := []string{
			"streams", "openConnections", "receivingConnections",
			"opened", "closed", "errors", "bytes", "packets",
			"opened/s", "closed/s", "errors/s", "bytes/s", "packets/s",
			"packetDelay",
			"pixels", "bytes/pixel", "packets/pixel",
			"bytes/connection", "packets/connection",
		}
		err := c.GetSink().Sample(
			&bitflow.Sample{
				Time:   time.Now(),
				Values: values,
			},
			&bitflow.Header{
				Fields: fields,
			})
		if err != nil {
			log.Errorln("Failed to sink stream statistics:", err)
		}
	}
}

type RunningStream struct {
	col     *StreamStatisticsCollector
	stopper golib.StopChan
	wg      sync.WaitGroup
	stream  *RtmpStream
}

func (c *RunningStream) start() {
	c.col.wg.Add(1)
	c.wg.Add(1)
	go func() {
		defer c.col.wg.Done()
		defer c.wg.Done()
		for !c.stopper.Stopped() {
			c.stopper.WaitTimeout(c.col.DelaySampler.distribution.Sample())
			c.handleStream()
		}
	}()
}

func (c *RunningStream) stop() {
	c.stopper.Stop()
	c.stream.Close()
	c.wg.Wait()
}

func (c *RunningStream) handleStream() {
	stream, err := c.col.Factory.OpenStream()
	c.stream = stream
	if err == ErrorNoURLs {
		log.Infof("No URLs available for streaming, sleeping for %v...", noUrlsSleepDuration)
		c.stopper.WaitTimeout(noUrlsSleepDuration)
		return
	} else if err != nil {
		log.Errorln("Error opening stream:", err)
		c.col.errors.Increment(1)
		return
	}

	// Make sure the stream is closed when we are finished
	defer c.stream.Close()

	pixels := int64(stream.Endpoint.pixels)
	c.col.opened.Increment(1)
	c.col.openConnections.Increment(1)
	defer c.col.openConnections.Increment(-1)
	received := false
	var previousPacketTime time.Time
	for !c.stopper.Stopped() {
		num, err := stream.Receive()
		if num > 0 {
			c.col.bytes.Increment(uint64(num))
			c.col.packets.Increment(1)
			now := time.Now()
			if !received {
				received = true
				c.col.receivingConnections.Increment(1)
				defer c.col.receivingConnections.Increment(-1)
				c.col.pixels.Increment(pixels)
				defer c.col.pixels.Increment(-pixels)
			} else {
				diff := now.Sub(previousPacketTime)
				c.col.packetDelay.Add(diff.Seconds())
			}
			previousPacketTime = now
		}
		if err == io.EOF {
			c.col.closed.Increment(1)
			return
		} else if err != nil {
			log.Errorln("Error reading from stream:", err)
			c.col.errors.Increment(1)
			c.col.closed.Increment(1)
			return
		}
	}
}
