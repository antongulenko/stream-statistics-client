package main

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitflow-stream/go-bitflow"
)

type TwoWayCounter struct {
	value int64
}

func (c *TwoWayCounter) Get() bitflow.Value {
	return bitflow.Value(atomic.LoadInt64(&c.value))
}

func (c *TwoWayCounter) Increment(val int64) {
	atomic.AddInt64(&c.value, val)
}

type IncrementedCounter struct {
	current  uint64
	previous uint64
}

func (c *IncrementedCounter) Get() bitflow.Value {
	return bitflow.Value(atomic.LoadUint64(&c.current))
}

func (c *IncrementedCounter) Increment(val uint64) {
	atomic.AddUint64(&c.current, val)
}

func (c *IncrementedCounter) ComputeDiff(timeDiff time.Duration) (bitflow.Value, bitflow.Value) {
	current := atomic.LoadUint64(&c.current)
	previous := c.previous
	c.previous = current
	diff := current - previous
	if current < previous {
		// Value overflow
		diff = math.MaxUint64 - previous + current
	}
	diffPerSecond := float64(diff) / timeDiff.Seconds()
	return bitflow.Value(current), bitflow.Value(diffPerSecond)
}

type AveragingCounter struct {
	count uint
	value float64
	lock  sync.Mutex
}

func (avg *AveragingCounter) Add(val float64) {
	avg.lock.Lock()
	defer avg.lock.Unlock()
	avg.count++
	avg.value += val
}

func (avg *AveragingCounter) ComputeAvg() bitflow.Value {
	avg.lock.Lock()
	count := avg.count
	value := avg.value
	avg.count = 0
	avg.value = 0
	avg.lock.Unlock()
	if count == 0 {
		return bitflow.Value(0)
	}
	return bitflow.Value(value / float64(count))
}
