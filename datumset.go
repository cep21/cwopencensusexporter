package cwopencensusexporter

import (
	"strings"

	"go.opencensus.io/metric/metricdata"
)

// datumSet associates opencensus metric data to the previous point for that data
type datumSet struct {
	previousDatumCache map[string]metricdata.Point
}

// get returns a point for metric data
func (d *datumSet) get(m *metricdata.Metric, ts *metricdata.TimeSeries) metricdata.Point {
	if ret, exists := d.previousDatumCache[hashMetric(m, ts)]; exists {
		return ret
	}
	return metricdata.Point{}
}

// set the previous point for a given time series
func (d *datumSet) set(m *metricdata.Metric, ts *metricdata.TimeSeries, p metricdata.Point) {
	if d.previousDatumCache == nil {
		d.previousDatumCache = make(map[string]metricdata.Point)
	}
	d.previousDatumCache[hashMetric(m, ts)] = p
}

// mustWrite will panic if err != nil
func mustWrite(_ int, err error) {
	if err != nil {
		panic(err)
	}
}

// mustNotErr will panic if err != nil
func mustNotErr(err error) {
	if err != nil {
		panic(err)

	}
}

// hashMetric converts a metric and time series into a unique string that we can hash on.  This allows us to look up
// previous point values for opencensus points
func hashMetric(m *metricdata.Metric, ts *metricdata.TimeSeries) string {
	var sb strings.Builder
	mustWrite(sb.WriteString(m.Descriptor.Name))
	mustNotErr(sb.WriteByte(0))
	if len(m.Descriptor.LabelKeys) != len(ts.LabelValues) {
		return sb.String()
	}
	for i := range m.Descriptor.LabelKeys {
		if !ts.LabelValues[i].Present {
			continue
		}
		mustWrite(sb.WriteString(m.Descriptor.LabelKeys[i].Key))
		mustNotErr(sb.WriteByte(0))
		mustWrite(sb.WriteString(ts.LabelValues[i].Value))
		mustNotErr(sb.WriteByte(0))
	}
	return sb.String()
}
