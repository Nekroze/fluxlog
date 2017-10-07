package fluxlog

import (
	"log"
	"time"

	influx "github.com/influxdata/influxdb/client/v2"
)

var queue = make(chan *influx.Point, 1000)
var QueueFlushInterval time.Duration = time.Second

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
			log.Printf("Failed to process %d fluxlog queue points due to error: %s", len(queue), err)
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

	bp, err := influx.NewBatchPoints(influx.BatchPointsConfig{
		Database:  db,
		Precision: precision,
	})
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

func enqueue(measurement string, fields map[string]interface{}, tags map[string]string) error {
	pt, err := influx.NewPoint(measurement, tags, fields, time.Now())
	if err != nil {
		return err
	}
	queue <- pt
	return nil
}
