package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	bitflow "github.com/antongulenko/go-bitflow"
	"github.com/antongulenko/go-bitflow-collector/cmd_helper"
	"github.com/antongulenko/golib"
	log "github.com/sirupsen/logrus"
)

const noUrlsSleepDuration = 5 * time.Second

func main() {
	os.Exit(do_main())
}

func do_main() int {
	parallelStreams := flag.Int("n", 1, "Number of parallel streams to start immediately")
	restartDelay := flag.Duration("restartDelay", 500*time.Millisecond, "Time before starting a new stream, when a stream ends (with or without error)")
	sinkInterval := flag.Duration("si", 1000*time.Millisecond, "Interval in which to send out stream statistics")
	timeout := flag.Duration("timeout", 5*time.Second, "Timeout for RTMP streams")

	cmd := cmd_helper.CmdDataCollector{DefaultOutput: "csv://-"}
	cmd.ParseFlags()
	if flag.NArg() == 0 {
		golib.Fatalln("Please provide positional arguments (at leat one) for the endpoints to stream from (will be chosen in round robin fashion)")
	}
	defer golib.ProfileCpu()()

	factory := &RtmpStreamFactory{
		URLs:            flag.Args(),
		TimeoutDuration: *timeout,
	}
	stats := &StreamStatisticsCollector{
		InitialStreams:     *parallelStreams,
		Factory:            factory,
		RestartDelay:       *restartDelay,
		SampleSinkInterval: *sinkInterval,
	}
	cmd.RestApis = append(cmd.RestApis, &SetUrlsRestApi{Col: stats})
	pipe := cmd.MakePipeline()
	pipe.Source = stats
	for _, str := range pipe.FormatLines() {
		log.Println(str)
	}
	return pipe.StartAndWait()
}

type StreamStatisticsCollector struct {
	bitflow.AbstractMetricSource

	InitialStreams     int
	Factory            *RtmpStreamFactory
	RestartDelay       time.Duration
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

func (c *StreamStatisticsCollector) Stop() {
	c.stopper.Stop()
	c.SetNumberOfStreams(0)
}

func (c *StreamStatisticsCollector) sinkSamples(wg *sync.WaitGroup) {
	defer wg.Done()
	defer c.CloseSink(wg)
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
		values := []bitflow.Value{
			// Meta values
			bitflow.Value(len(c.runningStreams)),
			c.openConnections.Get(),
			c.receivingConnections.Get(),
			// Absolute values
			opened, closed, errors, bytes, packets,
			// Values per second
			openedDiff, closedDiff, errorsDiff, bytesDiff, packetsDiff,
			// Average values
			packetDelay,
		}
		fields := []string{
			"streams", "openConnections", "receivingConnections",
			"opened", "closed", "errors", "bytes", "packets",
			"opened/s", "closed/s", "errors/s", "bytes/s", "packets/s",
			"packetDelay",
		}
		err := c.OutgoingSink.Sample(
			&bitflow.Sample{
				Time:   time.Now(),
				Values: values,
			},
			&bitflow.Header{
				Fields: fields,
			})
		if err != nil {
			log.Errorln("Failed to sink stream statistcs:", err)
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
			c.handleStream()
			c.stopper.WaitTimeout(c.col.RestartDelay)
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
		log.Println("No URLs available for streaming, sleeping for %v...")
		c.stopper.WaitTimeout(noUrlsSleepDuration)
		return
	} else if err != nil {
		log.Errorln("Error opening stream:", err)
		c.col.errors.Increment(1)
		return
	}
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
			if !received {
				received = true
				c.col.receivingConnections.Increment(1)
				defer c.col.receivingConnections.Increment(-1)
			} else {
				now := time.Now()
				diff := now.Sub(previousPacketTime)
				previousPacketTime = now
				c.col.packetDelay.Add(diff.Seconds())
			}
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
