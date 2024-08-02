// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package histogram // import "github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/aggregator/histogram"

import (
	"sort"
	"time"

	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/config"
	"github.com/elastic/opentelemetry-collector-components/connector/spanmetricsconnectorv2/internal/model"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// metricUnitToDivider gives a value that could used to divide the
// nano precision duration to the required unit specified in config.
var metricUnitToDivider = map[config.MetricUnit]float64{
	config.MetricUnitNs: float64(time.Nanosecond.Nanoseconds()),
	config.MetricUnitUs: float64(time.Microsecond.Nanoseconds()),
	config.MetricUnitMs: float64(time.Millisecond.Nanoseconds()),
	config.MetricUnitS:  float64(time.Second.Nanoseconds()),
}

// ExplicitBounds is a representation of explict bound histogram for
// calculating histograms for span durations.
type ExplicitBounds struct {
	// TODO (lahsivjar): Attribute hash collisions are not considered
	datapoints map[metricKey]map[[16]byte]*explicitHistogramDP
	timestamp  time.Time
}

// NewExplicitBounds creates a new instance of explicit bounds histogram.
func NewExplicitBounds() *ExplicitBounds {
	return &ExplicitBounds{
		datapoints: make(map[metricKey]map[[16]byte]*explicitHistogramDP),
		timestamp:  time.Now(),
	}
}

// Add adds a datapoint for a given metric definition to the histogram.
func (h *ExplicitBounds) Add(
	md model.MetricDef,
	srcAttrs pcommon.Map,
	spanDuration time.Duration,
) error {
	filteredAttrs := pcommon.NewMap()
	for _, definedAttr := range md.Attributes {
		if srcAttr, ok := srcAttrs.Get(definedAttr.Key); ok {
			srcAttr.CopyTo(filteredAttrs.PutEmpty(definedAttr.Key))
			continue
		}
		if definedAttr.DefaultValue.Type() != pcommon.ValueTypeEmpty {
			definedAttr.DefaultValue.CopyTo(filteredAttrs.PutEmpty(definedAttr.Key))
		}
	}

	// If all the configured attributes are not present in source
	// metric then don't count them.
	if filteredAttrs.Len() != len(md.Attributes) {
		return nil
	}

	key := metricKey{Name: md.Name, Desc: md.Description}
	if _, ok := h.datapoints[key]; !ok {
		h.datapoints[key] = make(map[[16]byte]*explicitHistogramDP)
	}

	var attrKey [16]byte
	if filteredAttrs.Len() > 0 {
		attrKey = pdatautil.MapHash(filteredAttrs)
	}

	if _, ok := h.datapoints[key][attrKey]; !ok {
		h.datapoints[key][attrKey] = newExplicitHistogramDP(filteredAttrs, md.Histogram.Explicit.Buckets)
	}

	value := float64(spanDuration.Nanoseconds()) / metricUnitToDivider[md.Unit]
	dp := h.datapoints[key][attrKey]
	dp.sum += value
	dp.count++
	dp.counts[sort.SearchFloat64s(dp.bounds, value)]++
	return nil
}

// Move moves the histogram for a given metric definition to a metric slice.
// Note that move also deletes the histogram representation after moving.
func (h *ExplicitBounds) Move(
	md model.MetricDef,
	dest pmetric.MetricSlice,
) {
	key := metricKey{Name: md.Name, Desc: md.Description}
	srcDps, ok := h.datapoints[key]
	if !ok || len(srcDps) == 0 {
		return
	}

	destMetric := dest.AppendEmpty()
	destMetric.SetName(md.Name)
	destMetric.SetDescription(md.Description)
	destHist := destMetric.SetEmptyHistogram()
	destHist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	destHist.DataPoints().EnsureCapacity(len(srcDps))
	for _, srcDp := range srcDps {
		destDp := destHist.DataPoints().AppendEmpty()
		srcDp.attrs.CopyTo(destDp.Attributes())
		destDp.ExplicitBounds().FromRaw(srcDp.bounds)
		destDp.BucketCounts().FromRaw(srcDp.counts)
		destDp.SetCount(srcDp.count)
		destDp.SetSum(srcDp.sum)
		// TODO determine appropriate start time
		destDp.SetTimestamp(pcommon.NewTimestampFromTime(h.timestamp))
	}
	// If there are two metric defined with the same key required by metricKey
	// then they will be aggregated within the same histogram and produced
	// together. Deleting the key ensures this while preventing duplicates.
	delete(h.datapoints, key)
}

// Size returns the number of datapoints in the histogram representation.
func (h *ExplicitBounds) Size() int {
	return len(h.datapoints)
}

// Reset resets the histogram for another usage. Note that timestamp is
// not updated as part of the reset.
func (h *ExplicitBounds) Reset() {
	clear(h.datapoints)
}

type metricKey struct {
	Name string
	Desc string
}

type explicitHistogramDP struct {
	attrs pcommon.Map

	sum   float64
	count uint64

	// bounds represents the explicitly defined boundaries for the histogram
	// bucket. The boundaries for a bucket at index i are:
	//
	// (-Inf, bounds[i]] for i == 0
	// (bounds[i-1], bounds[i]] for 0 < i < len(bounds)
	// (bounds[i-1], +Inf) for i == len(bounds)
	//
	// Based on above representation, a bounds of length n represents n+1 buckets.
	bounds []float64

	// counts represents the count values of histogram for each bucket. The sum of
	// counts across all buckets must be equal to the count variable. The length of
	// counts must be one greather than the length of bounds slice.
	counts []uint64
}

func newExplicitHistogramDP(attrs pcommon.Map, bounds []float64) *explicitHistogramDP {
	return &explicitHistogramDP{
		attrs:  attrs,
		bounds: bounds,
		counts: make([]uint64, len(bounds)+1),
	}
}
