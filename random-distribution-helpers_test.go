package main

import (
	testAssert "github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"
)

func compare(t *testing.T, expected DistributionSampler, actual DistributionSampler) {
	assert := testAssert.New(t)
	assert.Equal(expected, actual)
}

func parse(t *testing.T, distString string, expectedError bool) DistributionSampler {
	assert := testAssert.New(t)
	actual := DistributionSampler{}
	err := actual.Set(distString)
	if expectedError {
		assert.Error(err, distString)
	} else {
		assert.Nil(err, "Error while parsing legit distribution string", distString)
	}
	return actual
}

func TestConstantDistribution(t *testing.T) {
	constDistStringIdentifier := "const:"
	for value := -10; value <= 10; value++ {
		constDistString := constDistStringIdentifier + strconv.Itoa(value) + "s"
		if value < 0 {
			_ = parse(t, constDistString, true)
		} else {
			expected := DistributionSampler{
				distribution: &ConstDistribution{value: time.Second * time.Duration(value)},
			}
			actual := parse(t, constDistString, false)
			compare(t, expected, actual)
		}
	}
}

func TestEqualDistribution(t *testing.T) {
	equalDistStringIdentifier := "equal:"
	for min := -10; min <= 10; min++ {
		for max := min + 1; max <= 11; max++ {
			equalDistString := equalDistStringIdentifier + strconv.Itoa(min) + "s," + strconv.Itoa(max) + "s"
			if min < 0 || max < 0 {
				_ = parse(t, equalDistString, true)
			} else {
				expected := DistributionSampler{
					distribution: &EqualDistribution{
						min: time.Second * time.Duration(min),
						max: time.Second * time.Duration(max)},
				}
				actual := parse(t, equalDistString, false)
				compare(t, expected, actual)
			}
		}
	}
}

func TestNormalDistribution(t *testing.T) {
	normalDistStringIdentifier := "norm:"
	for mu := -10; mu <= 10; mu++ {
		for sigma := -10; sigma <= 10; sigma++ {
			normDistString := normalDistStringIdentifier + strconv.Itoa(mu) + "s," + strconv.Itoa(sigma) + "s"
			if mu < 0 || sigma < 0 {
				_ = parse(t, normDistString, true)
			} else {
				expected := DistributionSampler{
					distribution: &NormalDistribution{
						mu: time.Second * time.Duration(mu),
						sigma: time.Second * time.Duration(sigma)},
				}
				actual := parse(t, normDistString, false)
				compare(t, expected, actual)
			}
		}
	}
}

func TestWrongDistributionStrings(t *testing.T) {
	wrongs := []string{ "norm:1s,", "norm:1s", "equal:10s", "const:", "cost:1ms,5s"}
	for _, w := range wrongs {
		_ = parse(t, w, true)
	}
}
