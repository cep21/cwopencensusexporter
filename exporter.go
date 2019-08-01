package cwopencensusexporter

import (
	"context"
	"math"
	"time"

	"github.com/cep21/cwpagedmetricput"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"go.opencensus.io/metric/metricdata"
	"go.opencensus.io/metric/metricexport"
)

// Exporter understands how to implement the interface of metricexport.Exporter and turn a list of
// metricdata.Metric into cloudwatch.MetricDatum.  Those datum are fed into Sender for each call to ExportMetrics
type Exporter struct {
	// Sender is anything that can take cloudwatch.MetricDatum and send them to cloudwatch
	Sender MetricDatumSender
	// OnFailedSend is called with any metric datum that the sender fails to send
	OnFailedSend func(md *cloudwatch.MetricDatum, err error)
	// previousValues stores previous *metricdata.Metric points so that we can calculate differences and send those
	// to cloudwatch
	previousValues datumSet
}

var _ metricexport.Exporter = &Exporter{}

// ExportMetrics converts all metrics items into the appropriate *cloudwatch.MetricDatum and sends each to
// Sender.  It will ignore any metrics that Exporter cannot currently export and calls OnFailedSend on any failed sends.
func (e *Exporter) ExportMetrics(ctx context.Context, metrics []*metricdata.Metric) error {
	for _, metric := range metrics {
		for _, ts := range metric.TimeSeries {
			for _, p := range ts.Points {
				prevPoint := e.previousValues.get(metric, ts)
				dt := makeDatumFromMetric(prevPoint, metric, ts, p)
				if dt == nil {
					continue
				}
				e.previousValues.set(metric, ts, p)
				if err := e.Sender.SendMetricDatum(dt); err != nil {
					if e.OnFailedSend != nil {
						e.OnFailedSend(dt, err)
					}
				}
			}
		}
	}
	return nil
}

// ignorePrevious is true if a metric type should not be a delta of the previous value (this is usually gauges).
func ignorePrevious(metric metricdata.Type) bool {
	return metric == metricdata.TypeGaugeDistribution || metric == metricdata.TypeGaugeFloat64 || metric == metricdata.TypeGaugeInt64
}

// middleTime returns a pointer to a time value in the middle of prev and current
func middleTime(prev *time.Time, current *time.Time) *time.Time {
	if prev == nil {
		return current
	}
	if current == nil {
		return aws.Time(time.Now())
	}
	return aws.Time(prev.Add(current.Sub(*prev) / 2))
}

// makeDatumFromMetric converts a opencensus metric into a cloudwatch datum.  Returns nil if we are unable
// to create a datum for this point.
func makeDatumFromMetric(previous metricdata.Point, metric *metricdata.Metric, ts *metricdata.TimeSeries, p metricdata.Point) *cloudwatch.MetricDatum {
	// Empty out previous value if it should be ignored or is invalid
	if previous.Value == nil || ignorePrevious(metric.Descriptor.Type) {
		previous.Value = nil
		previous.Time = p.Time
	}
	ret := &cloudwatch.MetricDatum{
		MetricName: aws.String(metric.Descriptor.Name),
		Unit:       toCloudwatchUnit(metric.Descriptor.Unit),
		Dimensions: makeDimensions(metric, ts),
		Timestamp:  middleTime(&previous.Time, &p.Time),
	}
	switch v := p.Value.(type) {
	case int64:
		ret.Value = aws.Float64(float64(v))
		if asI, ok := previous.Value.(int64); ok {
			ret.Value = aws.Float64(float64(v - asI))
		}
	case float64:
		ret.Value = aws.Float64(v)
		if asI, ok := previous.Value.(float64); ok {
			ret.Value = aws.Float64(v - asI)
		}
	case *metricdata.Distribution:
		var prevDist *metricdata.Distribution
		if asD, ok := previous.Value.(*metricdata.Distribution); ok {
			prevDist = asD
		}
		ret = setDistribution(ret, v, prevDist)
	case *metricdata.Summary:
		ret = setSummary(ret, v.Snapshot)
	default:
		// For data types we don't understand, return nil to signal they are not a valid datum.
		return nil
	}
	return ret
}

// toCloudwatchUnit converts open census units into CloudWatch units
func toCloudwatchUnit(unit metricdata.Unit) *string {
	if unit == metricdata.UnitDimensionless {
		return nil
	}
	if unit == metricdata.UnitBytes {
		return aws.String("Bytes")
	}
	if unit == metricdata.UnitMilliseconds {
		return aws.String("Milliseconds")
	}
	// Just to be safe
	return nil
}

// setSummary converts a summary value's Snapshot into a datum.
func setSummary(datum *cloudwatch.MetricDatum, s metricdata.Snapshot) *cloudwatch.MetricDatum {
	if s.Count == 0 {
		return nil
	}
	if len(s.Percentiles) == 0 {
		return nil
	}
	min := math.Inf(1)
	max := math.Inf(-1)
	for k := range s.Percentiles {
		if k < min {
			min = k
		}
		if k > max {
			max = k
		}
	}
	if math.IsInf(min, 1) || math.IsInf(max, -1) {
		return nil
	}
	datum.StatisticValues = &cloudwatch.StatisticSet{
		Sum:         aws.Float64(s.Sum),
		SampleCount: aws.Float64(float64(s.Count)),
		Maximum:     aws.Float64(s.Percentiles[max]),
		Minimum:     aws.Float64(s.Percentiles[min]),
	}
	return datum
}

// setDistribution converts a distribution point into a datum
func setDistribution(datum *cloudwatch.MetricDatum, d *metricdata.Distribution, prev *metricdata.Distribution) *cloudwatch.MetricDatum {
	if d.BucketOptions == nil {
		return nil
	}
	if len(d.Buckets) != len(d.BucketOptions.Bounds)+1 {
		panic("I got this wrong")
	}
	var prevVal, prevCounts []*float64
	if prev != nil {
		prevVal, prevCounts = extractValueCounts(prev.BucketOptions.Bounds, prev.Buckets)
	}
	curVal, curCounts := extractValueCounts(d.BucketOptions.Bounds, d.Buckets)
	val, counts := diffValueCounts(prevVal, prevCounts, curVal, curCounts)
	if len(prevVal) != len(prevCounts) {
		panic("logic error")
	}
	if len(val) == 0 {
		return nil
	}
	if len(val) != len(counts) {
		panic("logic error")
	}
	if len(val) == 1 && *counts[0] == 1 {
		datum.Value = val[0]
		return datum
	}
	datum.Values = val
	datum.Counts = counts
	datum.StatisticValues = &cloudwatch.StatisticSet{
		SampleCount: aws.Float64(float64(d.Count)),
		Sum:         aws.Float64(d.Sum),
		// Note: I wish I didn't have to guess this.  Why can't they just tell me what the min and max actually are
		Maximum: guessMax(val, counts),
		Minimum: guessMin(val, counts),
	}
	if datum.StatisticValues.Minimum == nil {
		datum.StatisticValues = nil
	}
	if prev != nil {
		datum.StatisticValues.Sum = aws.Float64(*datum.StatisticValues.Sum - prev.Sum)
		datum.StatisticValues.SampleCount = aws.Float64(*datum.StatisticValues.SampleCount - float64(prev.Count))
	}
	if allOneValues(datum.Counts) {
		datum.Counts = nil
	}
	return datum
}

// diffValueCounts calculates the differce of bucket values from a previous point to the current point
func diffValueCounts(prevVal []*float64, prevCounts []*float64, currentVal []*float64, currentCounts []*float64) ([]*float64, []*float64) {
	if len(prevVal) == 0 {
		return currentVal, currentCounts
	}
	if len(prevVal) != len(prevCounts) {
		panic("logic error: prev lens")
	}
	if len(currentVal) != len(currentCounts) {
		panic("logic error: prev lens")
	}
	prevCountByValue := make(map[float64]float64)
	for i := range prevVal {
		prevCountByValue[*prevVal[i]] = *prevCounts[i]
	}
	for i := range currentVal {
		currentCounts[i] = aws.Float64(*currentCounts[i] - prevCountByValue[*currentVal[i]])
	}
	// Trim zeros
	newCount := make([]*float64, 0, len(currentVal))
	newVal := make([]*float64, 0, len(currentVal))
	for i := range currentCounts {
		if *currentCounts[i] != 0 {
			newCount = append(newCount, currentCounts[i])
			newVal = append(newVal, currentVal[i])
		}
	}
	return newVal, newCount
}

// guessMax attempts to guess a Max aggregation from a sorted list of bucket values.
func guessMax(val []*float64, counts []*float64) *float64 {
	for i := len(counts) - 1; i >= 0; i-- {
		if *counts[i] != 0 {
			return val[i]
		}
	}
	return nil
}

// guessMin attempts to guess a Min aggregation from a sorted list of bucket values.
func guessMin(val []*float64, counts []*float64) *float64 {
	for i := 0; i < len(counts); i++ {
		if *counts[i] != 0 {
			return val[i]
		}
	}
	return nil
}

// makeDimensions converts a time series's dimensions into cloudwatch dimensions.
func makeDimensions(m *metricdata.Metric, ts *metricdata.TimeSeries) []*cloudwatch.Dimension {
	uniqueDims := make(map[string]struct{})
	ret := make([]*cloudwatch.Dimension, 0, len(ts.LabelValues))
	for i, lv := range ts.LabelValues {
		if !lv.Present {
			continue
		}
		if len(m.Descriptor.LabelKeys) <= i {
			continue
		}
		lk := m.Descriptor.LabelKeys[i]
		if _, exists := uniqueDims[lk.Key]; exists {
			continue
		}
		uniqueDims[lk.Key] = struct{}{}
		ret = append(ret, &cloudwatch.Dimension{
			Name:  &lk.Key,
			Value: &lv.Value,
		})
	}
	return ret
}

// allOneValues returns true if vals is all == 1
func allOneValues(vals []*float64) bool {
	for _, v := range vals {
		if *v != 1 {
			return false
		}
	}
	return true
}

// extractValueCounts attempts to turn open census's buckets into aws's values and counts array
func extractValueCounts(bounds []float64, buckets []metricdata.Bucket) ([]*float64, []*float64) {
	counts := make([]*float64, 0, len(bounds))
	vals := make([]*float64, 0, len(buckets))
	for i := 0; i <= len(bounds); i++ {
		var boundStart float64
		var boundEnd float64
		if i == 0 {
			boundStart = 0
		} else {
			boundStart = bounds[i-1]
		}
		if i == len(bounds) {
			boundEnd = bounds[i-1]
		} else {
			boundEnd = bounds[i]
		}
		if boundStart > boundEnd {
			continue
		}
		bucketCount := buckets[i].Count
		if bucketCount <= 0 {
			continue
		}
		bucketMiddle := (boundStart + boundEnd) / 2
		vals = append(vals, &bucketMiddle)
		counts = append(counts, aws.Float64(float64(bucketCount)))
	}
	if len(counts) == 0 {
		return nil, nil
	}
	return vals, counts
}

// DefaultExporter returns a reasonable Exporter that you can attach to a Reader.  The exporter will send metrics
// to BatchMetricDatumSender.  You should call `go BatchMetricDatumSender.Run()` on the returned Sender and pass the
// Exporter to a Reader.
func DefaultExporter(client CloudWatchClient) (*Exporter, *BatchMetricDatumSender) {
	b := BatchMetricDatumSender{
		CloudWatchClient: &cwpagedmetricput.Pager{
			Client: client,
		},
	}
	return &Exporter{
		Sender: &b,
	}, &b
}
