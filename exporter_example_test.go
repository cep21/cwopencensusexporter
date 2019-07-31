package cwopencensusexporter_test

import (
	"context"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/cep21/cwopencensusexporter"
	"go.opencensus.io/metric/metricexport"
	"log"
)

func ExampleExporter_ExportMetrics() {
	awsClient := cloudwatch.New(session.Must(session.NewSession()))
	exporter, sender := cwopencensusexporter.DefaultExporter(awsClient)
	go func() {
		if err := sender.Run(); err != nil {
			log.Print(err)
		}
	}()
	defer func() {
		if err := sender.Shutdown(context.Background()); err != nil {
			log.Print(err)
		}
	}()
	ir, err := metricexport.NewIntervalReader(&metricexport.Reader{}, exporter)
	if err != nil {
		panic(err)
	}
	if err := ir.Start(); err != nil {
		panic(err)
	}
	defer ir.Stop()
}
