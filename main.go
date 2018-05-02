package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	bitflow "github.com/antongulenko/go-bitflow"
	collector "github.com/antongulenko/go-bitflow-collector"
	"github.com/antongulenko/go-bitflow-collector/cmd_helper"
	"github.com/antongulenko/golib"
	log "github.com/sirupsen/logrus"
)

func main() {
	os.Exit(do_main())
}

func do_main() int {
	parallelStreams := flag.Int("n", 1, "Number of parallel streams to start and measure")
	restartDelay := flag.Duration("restartDelay", 500*time.Millisecond, "Time before starting a new stream, when a stream ends (with or without error)")
	receiveBufferSize := flag.Uint("receiveBuffer", 1024*1024, "Number of bytes to read from each stream at a time")
	sinkInterval := flag.Duration("si", 1000*time.Millisecond, "Interval in which to send out stream statistics")
	doHttp := flag.Bool("http", false, "Start http streams instead of multimedia streams")

	cmd := cmd_helper.CmdDataCollector{DefaultOutput: "csv://-"}
	cmd.ParseFlags()
	if flag.NArg() == 0 {
		golib.Fatalln("Please provide positional arguments (at leat one) for the endpoints to stream from (will be chosen in round robin fashion)")
	}
	defer golib.ProfileCpu()()

	var factory StreamFactory
	streamFactory := URLStreamFactory{URLs: flag.Args()}

	if *doHttp {
		factory = &HttpStreamFactory{
			URLStreamFactory: streamFactory,
			ReceiveBuffer:    make([]byte, int(*receiveBufferSize)),
		}
	} else {
		factory = &MultimediaStreamFactory{
			URLStreamFactory:      streamFactory,
			ExpectedInitialErrors: 10,
		}
	}

	pipe := cmd.MakePipeline()
	pipe.Source = &StreamStatisticsCollector{
		Factory:            factory,
		ParallelStreams:    *parallelStreams,
		RestartDelay:       *restartDelay,
		SampleSinkInterval: *sinkInterval,
		ValueRingFactory: collector.ValueRingFactory{
			Length:   1000,
			Interval: time.Second,
		},
	}
	for _, str := range pipe.FormatLines() {
		log.Println(str)
	}
	return pipe.StartAndWait()
}

type StreamStatisticsCollector struct {
	bitflow.AbstractMetricSource

	collector.ValueRingFactory
	Factory            StreamFactory
	ParallelStreams    int
	RestartDelay       time.Duration
	SampleSinkInterval time.Duration

	stopper golib.StopChan

	// Stream statistics
	lock   sync.Mutex
	opened *collector.ValueRing
	closed *collector.ValueRing
	errors *collector.ValueRing
	bytes  *collector.ValueRing
}

func (c *StreamStatisticsCollector) String() string {
	return fmt.Sprintf("Measure %v stream(s) from %T", c.ParallelStreams, c.Factory)
}

func (c *StreamStatisticsCollector) Start(wg *sync.WaitGroup) golib.StopChan {
	c.stopper = golib.NewStopChan()
	c.opened = c.NewValueRing()
	c.closed = c.NewValueRing()
	c.errors = c.NewValueRing()
	c.bytes = c.NewValueRing()
	for i := 0; i < c.ParallelStreams; i++ {
		wg.Add(1)
		go c.handleStreamLoop(wg)
	}
	wg.Add(1)
	go c.sinkSamples(wg)
	return c.stopper
}

func (c *StreamStatisticsCollector) Stop() {
	c.stopper.Stop()
}

func (c *StreamStatisticsCollector) sinkSamples(wg *sync.WaitGroup) {
	defer wg.Done()
	for c.stopper.WaitTimeout(c.SampleSinkInterval) {
		values := []bitflow.Value{
			// Absolute values
			c.opened.GetHeadValue(),
			c.closed.GetHeadValue(),
			c.errors.GetHeadValue(),
			c.bytes.GetHeadValue(),
			// Values per second
			c.opened.GetDiff(),
			c.closed.GetDiff(),
			c.errors.GetDiff(),
			c.bytes.GetDiff(),
		}
		fields := []string{
			"opened", "closed", "errors", "bytes",
			"opened/s", "closed/s", "errors/s", "bytes/s",
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

func (c *StreamStatisticsCollector) handleStreamLoop(wg *sync.WaitGroup) {
	defer wg.Done()
	for !c.stopper.Stopped() {
		c.handleStream()
		c.stopper.WaitTimeout(c.RestartDelay)
	}
}

func (c *StreamStatisticsCollector) handleStream() {
	stream, err := c.Factory.OpenStream()
	if err != nil {
		log.Errorln("Error opening stream:", err)
		c.increment(c.errors, 1)
		return
	}
	c.increment(c.opened, 1)
	for !c.stopper.Stopped() {
		num, err := stream.Receive()
		if num > 0 {
			c.increment(c.bytes, num)
		}
		if err == io.EOF {
			c.increment(c.closed, 1)
			return
		} else if err != nil {
			log.Errorln("Error reading from stream:", err)
			c.increment(c.errors, 1)
			c.increment(c.closed, 1)
			return
		}
	}
}

func (c *StreamStatisticsCollector) increment(ring *collector.ValueRing, val int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	ring.IncrementValue(bitflow.Value(val))
}
