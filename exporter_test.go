package cwopencensusexporter

import (
	"context"
	"sync"
	"testing"

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

var _ CloudWatchClient = &mockCwclient{}

func TestFullFlow(t *testing.T) {
	sender := &BatchMetricDatumSender{
		CloudWatchClient: &mockCwclient{},
		OnFailedSend: func(datum []*cloudwatch.MetricDatum, err error) {
			panic(err)
		},
	}
	sender.init()
	e := Exporter{
		Sender: sender,
		OnFailedSend: func(md *cloudwatch.MetricDatum, err error) {
			panic(err)
		},
	}

	require.NoError(t, e.ExportMetrics(context.Background(), []*metricdata.Metric{{}}))
}
