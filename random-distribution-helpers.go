package main

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"
	log "github.com/sirupsen/logrus"
)

type RandomDistribution interface {
	Sample() time.Duration
	String() string
}

var _ RandomDistribution = RandomConstDistribution{}

type RandomConstDistribution struct {
	value time.Duration
}

func (constDist RandomConstDistribution) Sample() time.Duration {
	return constDist.value
}

func (constDist RandomConstDistribution) String() string {
	return fmt.Sprintf("Constant value: %d.", constDist.value)
}

var _ RandomDistribution = RandomEqualDistribution{}

type RandomEqualDistribution struct {
	min time.Duration
	max time.Duration
}

func (equalDist RandomEqualDistribution) Sample() time.Duration {
	return time.Duration(rand.Int63n(int64(equalDist.max) - int64(equalDist.min)) + int64(equalDist.min))
}

func (equalDist RandomEqualDistribution) String() string {
	return fmt.Sprintf("Equal distribution between %d and %d.", equalDist.min, equalDist.max)
}

var _ RandomDistribution = RandomNormalDistribution{}

type RandomNormalDistribution struct {
	mu    time.Duration
	sigma time.Duration
}

func (normDist RandomNormalDistribution) Sample() time.Duration {
	value := math.Round(rand.NormFloat64() * float64(normDist.sigma) + float64(normDist.mu))
	return time.Duration(value)
}

func (normDist RandomNormalDistribution) String() string {
	return fmt.Sprintf("Normal distribution with mean %d and standard deviation %d.", normDist.mu, normDist.sigma)
}

type RandomDistributionSampler struct {
	distribution	RandomDistribution
}

func (distSampler RandomDistributionSampler) String() string {
	return distSampler.distribution.String()
}

func (distSampler RandomDistributionSampler) Set(value string) error {
	formatErr := "Invalid random argument format. Please use format [const|equal|norm]:[param1, param2,...]. Reason: %v"
	if len(value) == 0 || !strings.Contains(value, ":") {
		return fmt.Errorf(formatErr, "Distribution type and parameters must be devided by ':'.")
	}
	typeAndParams := strings.Split(value, ":")
	if len(typeAndParams) != 2 {
		return fmt.Errorf(formatErr, "Missing distribution parameters.")
	}
	params := strings.Split(typeAndParams[1], ",")
	switch typeAndParams[0] { // Check the distribution type identifier
	case "const": // Parse values for constant distribution
		if len(params) != 1 {
			return fmt.Errorf(formatErr, "Constant distribution expects exactly one parameter but got %v.", len(params))
		}
		if value, err := time.ParseDuration(typeAndParams[1]);  err == nil {
			distSampler.distribution = RandomConstDistribution{value: value}
		} else {
			return fmt.Errorf(formatErr, err)
		}
	case "equal": // Parse values for equal distribution
		if len(params) != 2 {
			return fmt.Errorf(formatErr, "Equal distribution expects exactly two parameters but got %v.", len(params))
		} else {
			min, err := time.ParseDuration(params[0])
			if err != nil {
				return fmt.Errorf(formatErr, err)
			}
			max, err := time.ParseDuration(params[1])
			if err != nil {
				return fmt.Errorf(formatErr, err)
			}
			distSampler.distribution = RandomEqualDistribution{min: min, max: max}
		}
	case "norm": // Parse values for normal distribution
		if len(params) != 2 {
			return fmt.Errorf(formatErr, "Normal distribution expects exactly two parameters but got %v.", len(params))
		} else {
			mu, err := time.ParseDuration(params[0])
			if err != nil {
				return fmt.Errorf(formatErr, err)
			}
			sigma, err := time.ParseDuration(params[1])
			if err != nil {
				return fmt.Errorf(formatErr, err)
			}
			distSampler.distribution = RandomNormalDistribution{mu: mu, sigma: sigma}
		}
	default:
		return fmt.Errorf(formatErr, "Unknown distribution type identifier %v.", typeAndParams[0])
	}
	log.Printf("Successfully parsed distribution parameter %v. Result: %v", value, distSampler.distribution.String())

	return nil
}