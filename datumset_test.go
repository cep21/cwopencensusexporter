package cwopencensusexporter

import (
	"testing"
	"time"

	"go.opencensus.io/metric/metricdata"

	"github.com/stretchr/testify/require"
)

func Test_datumSet(t *testing.T) {
	m := &metricdata.Metric{
		Descriptor: metricdata.Descriptor{
			Name: "testing",
			LabelKeys: []metricdata.LabelKey{
				{
					Key: "name",
				},
			},
		},
	}
	ts := &metricdata.TimeSeries{
		LabelValues: []metricdata.LabelValue{
			{
				Value:   "john",
				Present: true,
			},
		},
	}
	p := metricdata.Point{
		Time:  time.Now(),
		Value: int64(3),
	}
	var x datumSet
	require.Nil(t, x.get(m, ts).Value)
	x.set(m, ts, p)
	require.Equal(t, int64(3), x.get(m, ts).Value.(int64))
}
