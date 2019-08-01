# cwopencensusexporter

[![Build Status](https://travis-ci.org/cep21/cwopencensusexporter.svg?branch=master)](https://travis-ci.org/cep21/cwopencensusexporter)
[![GoDoc](https://godoc.org/github.com/cep21/cwopencensusexporter?status.svg)](https://godoc.org/github.com/cep21/cwopencensusexporter)
[![Coverage Status](https://coveralls.io/repos/github/cep21/cwopencensusexporter/badge.svg)](https://coveralls.io/github/cep21/cwopencensusexporter)

cwopencensusexporter implements the opencensus Exporter API and allows you to send open census metrics to cloudwatch

# Example

```go
	awsClient := cloudwatch.New(session.Must(session.NewSession()))
	exporter, sender := cwopencensusexporter.DefaultExporter(awsClient)
	go sender.Run()
	defer sender.Shutdown(context.Background())
	ir, err := metricexport.NewIntervalReader(&metricexport.Reader{}, exporter)
	if err != nil {
		panic(err)
	}
	if err := ir.Start(); err != nil {
		panic(err)
	}
	defer ir.Stop()
```

# Limitations

The open census API does not fully allow first class CloudWatch support.  There are a few limitations causing this.

1. CloudWatch expects to send aggregations of Max or Min across a time window.  For example, the PutMetricData API
 for CloudWatch, when working on aggregations, wants you to specify the maximum and minimum value seen in a 60 second
 time window.  This is not possible with the input data of `metricdata.Distribution`.  While I can try to estimate a
 minimum or maximum value given the buckets, this isn't the true min or max that could otherwise be aggregated easily
 as points are ingested.
2. CloudWatch wants values aggregated inside a time window.  For example with 60 second aggregations, a point seen at 59.9 seconds should be in an
 aggregation for time window 0, while a point seen at 60.1 seconds should be in an aggregation for time window 1.  The
 API for `metricexport.Exporter` does not split aggregations across time windows.  The best I can do is try to align
 calls to `Exporter.ExportMetrics` to a time boundary (call at exactly 60 seconds then exactly at 120 seconds, etc).  This
 is unlikely to handle corner cases.  The best way to get data with the most fidelity would be to aggregate calls into
 buckets when they are added to `stats.Record`.
3. Some types, like `metricdata.Summary`, do not translate to CloudWatch metrics and I am unable to provide a good user 
 experience for data submitted to `cwopencensusexporter.Exporter.ExportMetrics` that are of type `metricdata.Summary`.
 An ideal experience for CloudWatch users would be to never let data get put into a `metricdata.Summary` and instead
 bucket them at the level of `stats.Record`.
4. The layers of abstraction inside opencensus create unnecessary memory allocation that
 could be avoided with a system designed for CloudWatch's aggregation API.
5. Open census buckets include the concept of [...inf] as the last range in their buckets.  There's no
 way to represent this range for CloudWatch.  Ideally open census would be able to track maximum (and minimum)
 values inside a time window so that I could use the min (or max) in the range of my buckets, rather than
 just assume infinity.

# Contributing

Make sure your tests pass CI/CD pipeline which includes running `make fix lint test` locally.
You'll need an AWS account to verify integration tests, which should also pass `make integration_test`.
I recommend opening a github issue to discuss your ideas before contributing code.
