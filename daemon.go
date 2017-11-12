package fluxlog

import (
	"log"
	"time"

	influx "github.com/influxdata/influxdb/client/v2"
)

var queueSize int = 10000
var queue = make(chan *influx.Point, queueSize)

// The interval between each buffer flush and send to influx as a batch of points
var QueueFlushInterval time.Duration = time.Second

// The rate at which consecutive connection errors should be logged. By default, if
// influxdb is down for 2 hours your logs should have only 2 connection error messages
// show the problem persists.
var ErrorLogRate time.Duration = time.Hour
var logNextErr int64

// When set to false fluxlog write functions buffer but block the bufer is full it will
// block until there is space. When set to true the queuing will happen in a goroutine
// that blocks but allows the caller to continue operating as though it went through
// immediately.
var AsyncBufferWhenFull bool

// If not set the default policy for the database will be used
var RetentionPolicy string

var batchConfig influx.BatchPointsConfig

func getBatchConfig() influx.BatchPointsConfig {
	if batchConfig.Database != DB ||
	batchConfig.Precision != Precision ||
	(len(RetentionPolicy) > 0 && batchConfig.RetentionPolicy != RetentionPolicy) {
		batchConfig = influx.BatchPointsConfig{
			Database: DB,
			Precision: Precision,
		}
		if len(RetentionPolicy) > 0 {
			batchConfig.RetentionPolicy = RetentionPolicy
		}
	}
	return batchConfig
}

func init() {
	go daemon()
}

func requeue(points []*influx.Point) {
	for _, p := range points {
		queue <- p
	}
}

func daemon() {
	for {
		err := ProcessQueue()
		if err != nil {
			if time.Now().Unix() >= logNextErr {
				log.Printf("Failed to process %d fluxlog queue points due to error: %s", len(queue), err)
				logNextErr = time.Now().Add(time.Hour).Unix()
			}
		} else if logNextErr > 0 {
			log.Printf("fluxlog successfully sending influxdb points again")
			logNextErr = 0
		}
		time.Sleep(QueueFlushInterval)
	}
}

func ProcessQueue() (err error) {
	length := len(queue)
	if length == 0 {
		return
	}

	err = ConnectInflux()
	if err != nil {
		return
	}

	bp, err := influx.NewBatchPoints(getBatchConfig())
	if err != nil {
		return
	}

	for i := 0; i < length; i++ {
		bp.AddPoint(<-queue)
	}

	err = client.Write(bp)
	if err != nil {
		requeue(bp.Points())
	}
	return
}

func queueAdd(point *influx.Point) {
	queue <- point
}

func tryNonAsyncQueue(point *influx.Point) {
	if AsyncBufferWhenFull && len(queue) >= queueSize {
		go queueAdd(point)
	} else {
		queueAdd(point)
	}
}

func enqueue(measurement string, fields map[string]interface{}, tags map[string]string) (err error) {
	pt, err := influx.NewPoint(measurement, tags, fields, time.Now())
	if err == nil {
		tryNonAsyncQueue(pt)
	}
	return
}
