package cwopencensusexporter

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awsutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

// MetricDatumSender is anything that can send datum somewhere
type MetricDatumSender interface {
	// SendMetricDatum should not block.  It should queue the datum for sending, or just send it.
	// It should not modify the input datum
	// but can assume the input datum is immutable.  Return an error if unable to send this datum correctly.
	SendMetricDatum(md *cloudwatch.MetricDatum) error
}

// CloudWatchClient is anything that can receive CloudWatch metrics as documented by CloudWatch's public API constraints.
type CloudWatchClient interface {
	// PutMetricDataWithContext should match the contract of cloudwatch.CloudWatch.PutMetricDataWithContext
	PutMetricDataWithContext(aws.Context, *cloudwatch.PutMetricDataInput, ...request.Option) (*cloudwatch.PutMetricDataOutput, error)
}

var _ CloudWatchClient = &cloudwatch.CloudWatch{}

// BatchMetricDatumSender aggregates datum into a channel and sends them to cloudwatch
type BatchMetricDatumSender struct {
	// CloudWatchClient is anything that can send datum to cloudwatch.  It should probably be cwpagedmetricput.Pager so
	// you can take care of batching large requests
	CloudWatchClient CloudWatchClient
	// BatchDelay is how long to wait between getting one value and waiting for a batch to fill up
	BatchDelay time.Duration
	// BatchSize is the maximum number of Datum to send to a single call to CloudWatchClient
	BatchSize int
	// Namespace is the cloudwatch namespace attached to the datum
	Namespace string
	// OnFailedSend is called on any failure to send datum to CloudWatchClient
	OnFailedSend func(datum []*cloudwatch.MetricDatum, err error)
	tosend       chan *cloudwatch.MetricDatum
	onClose      chan struct{}
	startDone    chan struct{}
	once         sync.Once
}

func (b *BatchMetricDatumSender) init() {
	b.once.Do(func() {
		b.startDone = make(chan struct{})
		b.onClose = make(chan struct{})
		b.tosend = make(chan *cloudwatch.MetricDatum, 1024)
	})
}

var _ MetricDatumSender = &BatchMetricDatumSender{}

// Run executes the batch datum sender.  You should probably execute this inside a goroutine.  It blocks until Shutdown
func (b *BatchMetricDatumSender) Run() error {
	b.init()
	defer close(b.startDone)
	for {
		var first *cloudwatch.MetricDatum
		select {
		case <-b.onClose:
			return nil
		case first = <-b.tosend:
		}
		finishSending := time.After(b.batchDelay())
		toSend := make([]*cloudwatch.MetricDatum, 0, b.batchSize())
		toSend = append(toSend, first)
	forloop:
		for len(toSend) < b.batchSize() {
			select {
			case next := <-b.tosend:
				toSend = append(toSend, next)
			case <-finishSending:
				break forloop
			}
		}
		log.Println("sending to cloudwatch", awsutil.Prettify(toSend))
		_, err := b.CloudWatchClient.PutMetricDataWithContext(context.Background(), &cloudwatch.PutMetricDataInput{
			MetricData: toSend,
			Namespace:  b.namespace(),
		})
		if err != nil && b.OnFailedSend != nil {
			b.OnFailedSend(toSend, err)
		}
	}
}

// Shutdown stops the sender once it has been started.  Blocks until either Run finishes, or ctx dies.
func (b *BatchMetricDatumSender) Shutdown(ctx context.Context) error {
	b.init()
	close(b.onClose)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.startDone:
	}
	return b.flush(ctx)
}

// SendMetricDatum queues a datum for sending to cloudwatch
func (b *BatchMetricDatumSender) SendMetricDatum(md *cloudwatch.MetricDatum) error {
	select {
	case b.tosend <- md:
		return nil
	default:
		return errors.New("tosend channel full")
	}
}

func (b *BatchMetricDatumSender) batchSize() int {
	if b.BatchSize == 0 {
		return 20
	}
	return b.BatchSize
}

func (b *BatchMetricDatumSender) batchDelay() time.Duration {
	if b.BatchDelay == 0 {
		return time.Second
	}
	return b.BatchDelay
}

func (b *BatchMetricDatumSender) namespace() *string {
	if b.Namespace == "" {
		return aws.String("custom")
	}
	return &b.Namespace
}

func (b *BatchMetricDatumSender) flush(ctx context.Context) error {
	for {
		var first *cloudwatch.MetricDatum
		select {
		case <-ctx.Done():
			return ctx.Err()
		case first = <-b.tosend:
		default:
			return nil
		}
		toSend := make([]*cloudwatch.MetricDatum, 0, b.batchSize())
		toSend = append(toSend, first)
	forloop:
		for len(toSend) < b.batchSize() {
			select {
			case next := <-b.tosend:
				toSend = append(toSend, next)
			default:
				break forloop
			}
		}
		_, err := b.CloudWatchClient.PutMetricDataWithContext(ctx, &cloudwatch.PutMetricDataInput{
			MetricData: toSend,
			Namespace:  b.namespace(),
		})
		if err != nil && b.OnFailedSend != nil {
			b.OnFailedSend(toSend, err)
		}
	}
}
