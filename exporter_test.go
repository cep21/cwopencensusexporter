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
		case <-time.After(time.Millisecond * 5):
			if m.check(check) {
				return nil
			}
		}
	}
}

var _ CloudWatchClient = &mockCwclient{}

func checkSingleGauge(mc *mockCwclient, e *Exporter) func(t *testing.T) {
	return func(t *testing.T) {
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
		require.NoError(t, e.ExportMetrics(context.Background(), []*metricdata.Metric{m1}))
		require.NoError(t, mc.wait(context.Background(), func(m *cloudwatch.MetricDatum) bool {
			return *m.MetricName == "m1"
		}))
	}
}

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
	require.NoError(t, e.ExportMetrics(context.Background(), []*metricdata.Metric{{}}))
	t.Run("single_float", checkSingleGauge(mc, &e))
	t.Run("first_dist", checkFirstDist(mc, &e))
	t.Run("second_dist", checkSecondDist(mc, &e))
	t.Run("third_dist", checkThirdDist(mc, &e))
}

func checkFirstDist(mc *mockCwclient, e *Exporter) func(t *testing.T) {
	return func(t *testing.T) {
		m3 := &metricdata.Metric{
			Descriptor: metricdata.Descriptor{
				Name: "m2",
				Type: metricdata.TypeCumulativeDistribution,
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
							Value:   "jack",
							Present: true,
						},
					},
					StartTime: time.Now(),
					Points: []metricdata.Point{
						{
							Time: time.Now(),
							Value: &metricdata.Distribution{
								Count:                 10,
								Sum:                   14,
								SumOfSquaredDeviation: 0,
								BucketOptions: &metricdata.BucketOptions{
									Bounds: []float64{1, 2},
								},
								Buckets: []metricdata.Bucket{
									{
										Count: 0,
									},
									{
										Count: 4,
									},
									{
										Count: 5,
									},
								},
							},
						},
					},
				},
			},
		}
		require.NoError(t, e.ExportMetrics(context.Background(), []*metricdata.Metric{m3}))
		require.NoError(t, mc.wait(context.Background(), func(m *cloudwatch.MetricDatum) bool {
			return *m.MetricName == "m2" && m.Values != nil &&
				*m.Values[0] == 1.5 && *m.Counts[0] == 4
		}))
	}
}

func checkSecondDist(mc *mockCwclient, e *Exporter) func(t *testing.T) {
	return func(t *testing.T) {
		m3 := &metricdata.Metric{
			Descriptor: metricdata.Descriptor{
				Name: "m2",
				Type: metricdata.TypeCumulativeDistribution,
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
							Value:   "jack",
							Present: true,
						},
					},
					StartTime: time.Now(),
					Points: []metricdata.Point{
						{
							Time: time.Now(),
							Value: &metricdata.Distribution{
								Count:                 11,
								Sum:                   15,
								SumOfSquaredDeviation: 0,
								BucketOptions: &metricdata.BucketOptions{
									Bounds: []float64{1, 2},
								},
								Buckets: []metricdata.Bucket{
									{
										Count: 0,
									},
									{
										Count: 5,
									},
									{
										Count: 5,
									},
								},
							},
						},
					},
				},
			},
		}
		require.NoError(t, e.ExportMetrics(context.Background(), []*metricdata.Metric{m3}))
		require.NoError(t, mc.wait(context.Background(), func(m *cloudwatch.MetricDatum) bool {
			return *m.MetricName == "m2" && m.Value != nil && *m.Value == 1.5
		}))
	}
}

func checkThirdDist(mc *mockCwclient, e *Exporter) func(t *testing.T) {
	return func(t *testing.T) {
		m3 := &metricdata.Metric{
			Descriptor: metricdata.Descriptor{
				Name: "m2",
				Type: metricdata.TypeCumulativeDistribution,
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
							Value:   "jack",
							Present: true,
						},
					},
					StartTime: time.Now(),
					Points: []metricdata.Point{
						{
							Time: time.Now(),
							Value: &metricdata.Distribution{
								Count:                 13,
								Sum:                   17,
								SumOfSquaredDeviation: 0,
								BucketOptions: &metricdata.BucketOptions{
									Bounds: []float64{1, 2},
								},
								Buckets: []metricdata.Bucket{
									{
										Count: 0,
									},
									{
										Count: 7,
									},
									{
										Count: 5,
									},
								},
							},
						},
					},
				},
			},
		}
		require.NoError(t, e.ExportMetrics(context.Background(), []*metricdata.Metric{m3}))
		require.NoError(t, mc.wait(context.Background(), func(m *cloudwatch.MetricDatum) bool {
			return *m.MetricName == "m2" && m.Values != nil &&
				*m.Values[0] == 1.5 && *m.Counts[0] == 2
		}))
	}
}
