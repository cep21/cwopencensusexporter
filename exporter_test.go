package cwopencensusexporter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/metric/metricdata"
)

type mockCwclient struct {
	calls []*cloudwatch.PutMetricDataInput
	mu    sync.Mutex
}

func (m *mockCwclient) PutMetricDataWithContext(ctx aws.Context, in *cloudwatch.PutMetricDataInput, opts ...request.Option) (*cloudwatch.PutMetricDataOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, in)
	return &cloudwatch.PutMetricDataOutput{}, nil
}

func (m *mockCwclient) check(check func(m *cloudwatch.MetricDatum) bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range m.calls {
		for _, d := range c.MetricData {
			if check(d) {
				return true
			}
		}
	}
	return false
}

func (m *mockCwclient) wait(ctx context.Context, check func(m *cloudwatch.MetricDatum) bool) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
			if m.check(check) {
				return nil
			}
		}
	}
}

var _ CloudWatchClient = &mockCwclient{}

func TestFullFlow(t *testing.T) {
	mc := &mockCwclient{}
	sender := &BatchMetricDatumSender{
		CloudWatchClient: mc,
		OnFailedSend: func(datum []*cloudwatch.MetricDatum, err error) {
			panic(err)
		},
	}
	sender.init()
	go func() {
		if err := sender.Run(); err != nil {
			panic(err)
		}
	}()
	defer func() {
		if err := sender.Shutdown(context.Background()); err != nil {
			panic(err)
		}
	}()
	e := Exporter{
		Sender: sender,
		OnFailedSend: func(md *cloudwatch.MetricDatum, err error) {
			panic(err)
		},
	}

	m1 := &metricdata.Metric{
		Descriptor: metricdata.Descriptor{
			Name: "m1",
			Type: metricdata.TypeGaugeFloat64,
			LabelKeys: []metricdata.LabelKey{
				{
					Key: "name",
				},
			},
		},
		Resource: nil,
		TimeSeries: []*metricdata.TimeSeries{
			{
				LabelValues: []metricdata.LabelValue{
					{
						Value:   "john",
						Present: true,
					},
				},
				StartTime: time.Now(),
				Points: []metricdata.Point{
					{
						Time:  time.Now(),
						Value: 1.0,
					},
				},
			},
		},
	}
	require.NoError(t, e.ExportMetrics(context.Background(), []*metricdata.Metric{{}}))
	require.NoError(t, e.ExportMetrics(context.Background(), []*metricdata.Metric{m1}))
	require.NoError(t, mc.wait(context.Background(), func(m *cloudwatch.MetricDatum) bool {
		return *m.MetricName == "m1"
	}))
}
