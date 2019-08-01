package main

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"
	log "github.com/sirupsen/logrus"
)

type Distribution interface {
	Sample() time.Duration
	String() string
}

var _ Distribution = &ConstDistribution{}

type ConstDistribution struct {
	value time.Duration
}

func (constDist *ConstDistribution) Sample() time.Duration {
	return constDist.value
}

func (constDist *ConstDistribution) String() string {
	return fmt.Sprintf("Constant value: %v.", constDist.value)
}

var _ Distribution = &EqualDistribution{}

type EqualDistribution struct {
	min time.Duration
	max time.Duration
}

func (equalDist *EqualDistribution) Sample() time.Duration {
	return time.Duration(rand.Int63n(int64(equalDist.max)-int64(equalDist.min)) + int64(equalDist.min))
}

func (equalDist *EqualDistribution) String() string {
	return fmt.Sprintf("Equal distribution between %v and %v.", equalDist.min, equalDist.max)
}

var _ Distribution = &NormalDistribution{}

type NormalDistribution struct {
	mu    time.Duration
	sigma time.Duration
}

func (normDist *NormalDistribution) Sample() time.Duration {
	value := math.Round(rand.NormFloat64()*float64(normDist.sigma) + float64(normDist.mu))
	return time.Duration(value)
}

func (normDist *NormalDistribution) String() string {
	return fmt.Sprintf("Normal distribution with mean %v ms and standard deviation %v.",
		int64(normDist.mu/time.Millisecond), normDist.sigma)
}

type DistributionSampler struct {
	distribution Distribution
}

func (distSampler *DistributionSampler) String() string {
	if distSampler.distribution != nil {
		return distSampler.distribution.String()
	} else {
		return "<nil>"
	}
}

func (distSampler *DistributionSampler) Set(value string) error {
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
		if value, err := parseDuration(typeAndParams[1]); err == nil {
			distSampler.distribution = &ConstDistribution{value: value}
		} else {
			return fmt.Errorf(formatErr, err)
		}
	case "equal": // Parse values for equal distribution
		if len(params) != 2 {
			return fmt.Errorf(formatErr, "Equal distribution expects exactly two parameters but got %v.", len(params))
		} else {
			min, err := parseDuration(params[0])
			if err != nil {
				return fmt.Errorf(formatErr, err)
			}
			max, err := time.ParseDuration(params[1])
			if err != nil {
				return fmt.Errorf(formatErr, err)
			}
			distSampler.distribution = &EqualDistribution{min: min, max: max}
		}
	case "norm": // Parse values for normal distribution
		if len(params) != 2 {
			return fmt.Errorf(formatErr, "Normal distribution expects exactly two parameters but got %v.", len(params))
		} else {
			mu, err := parseDuration(params[0])
			if err != nil {
				return fmt.Errorf(formatErr, err)
			}
			sigma, err := parseDuration(params[1])
			if err != nil {
				return fmt.Errorf(formatErr, err)
			}
			distSampler.distribution = &NormalDistribution{mu: mu, sigma: sigma}
		}
	default:
		return fmt.Errorf(formatErr, "Unknown distribution type identifier %v.", typeAndParams[0])
	}
	log.Printf("Successfully parsed distribution parameter %v. Result: %v", value, distSampler.distribution.String())

	return nil
}

func parseDuration(value string) (time.Duration, error) {
	timeValue, err := time.ParseDuration(value)
	if err != nil && timeValue >= 0 {
		return -1, err
	} else if err == nil && timeValue < 0 {
		return -1, fmt.Errorf("Distribution argument must be a positive time value but actually is %v", timeValue)
	}
	return timeValue, nil
}
