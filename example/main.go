package main

import (
	"context"
	"github.com/cep21/cwpagedmetricput"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opencensus.io/metric/metricexport"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/cep21/cwopencensusexporter"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	exampleDistIn  = stats.Float64("example/dist", "The latency in milliseconds per REPL loop", stats.UnitMilliseconds)
	exampleCountIn = stats.Int64("example/count", "The number of lines read in", stats.UnitDimensionless)
	exampleLastIn  = stats.Int64("example/last", "The number of errors encountered", stats.UnitDimensionless)
	exampleSumIn   = stats.Int64("example/sum", "The number of errors encountered", stats.UnitDimensionless)
)

var (
	keyMethod = tag.MustNewKey("method")
)

// Views for the stats quickstart.
var (
	exampleDist = &view.View{
		Name:        "demo/example_dist",
		Measure:     exampleDistIn,
		Description: "sample distribution",
		Aggregation: view.Distribution(5, 10, 30, 45, 60, 70, 80, 90, 95, 98),
		TagKeys:     []tag.Key{keyMethod}}

	exampleCount = &view.View{
		Name:        "demo/example_count",
		Measure:     exampleCountIn,
		Description: "example count",
		Aggregation: view.Count(),
	}

	exampleSum = &view.View{
		Name:        "demo/example_sum",
		Measure:     exampleSumIn,
		Description: "Example sum",
		Aggregation: view.Sum(),
	}

	exampleLast = &view.View{
		Name:        "demo/example_last",
		Measure:     exampleLastIn,
		Description: "example last",
		Aggregation: view.LastValue(),
	}
)

func main() {
	awsClient := cloudwatch.New(session.Must(session.NewSession()))
	log.Println("made a client")
	exporter, sender := cwopencensusexporter.DefaultExporter(awsClient)
	exporter.OnFailedSend = func(md *cloudwatch.MetricDatum, err error) {
		log.Println("Failed to send", *md.MetricName, err)
	}
	sender.OnFailedSend = func(datum []*cloudwatch.MetricDatum, err error) {
		log.Println("failed to send many datum", err)
	}
	sender.CloudWatchClient.(*cwpagedmetricput.Pager).Config.OnDroppedDatum = func(datum *cloudwatch.MetricDatum) {
		log.Println("Had to drop datum")
	}
	ir, err := metricexport.NewIntervalReader(&metricexport.Reader{}, exporter)
	if err != nil {
		panic(err)
	}
	ir.ReportingInterval = time.Second * 3
	if err := ir.Start(); err != nil {
		panic(err)
	}
	defer ir.Stop()

	log.Println("made a exporter/sender")
	go func() {
		if err := sender.Run(); err != nil {
			log.Println(err)
		}
	}()
	defer func() {
		if err := sender.Shutdown(context.Background()); err != nil {
			log.Println(err)
		}
	}()
	log.Println("view register")
	if err := view.Register(exampleDist, exampleCount, exampleSum, exampleLast); err != nil {
		log.Fatal(err)
	}

	log.Println("export register")
	done := make(chan struct{})
	go sendCount(done)
	go sendLast(done)
	go sendSum(done)
	go sendDistribution(done)
	log.Println("waiting on signal")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	log.Println("close after signal")
	close(done)
}

func sendDistribution(done chan struct{}) {
	ctx := context.Background()
	ctx, err := tag.New(ctx, tag.Insert(keyMethod, "atag"))
	if err != nil {
		panic(err)
	}
	i := int64(0)
	for {
		select {
		case <-done:
			return
		case <-time.After(time.Millisecond * 10):
			i++
			if i == 100 {
				i = 0
			}
			stats.Record(ctx, exampleDistIn.M(float64(i)))
		}
	}
}

func sendCount(done chan struct{}) {
	for {
		select {
		case <-done:
			return
		case <-time.After(time.Millisecond * 10):
			stats.Record(context.Background(), exampleCountIn.M(1))
		}
	}
}

func sendLast(done chan struct{}) {
	i := int64(0)
	for {
		select {
		case <-done:
			return
		case <-time.After(time.Millisecond * 10):
			i++
			stats.Record(context.Background(), exampleLastIn.M(i))
		}
	}
}

func sendSum(done chan struct{}) {
	for {
		select {
		case <-done:
			return
		case <-time.After(time.Millisecond * 10):
			stats.Record(context.Background(), exampleSumIn.M(2))
		}
	}
}
