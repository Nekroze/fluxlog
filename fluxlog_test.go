package fluxlog

import (
	"encoding/json"
	"math/rand"
	"testing"
	"time"
)

func configure() {
	ChangeGlobalTags(map[string]string{"env": "test"})
	SaveMetadata(true)
	SetAddress("http://storage:8086")
}

func TestWrite(t *testing.T) {
	configure()              // setup influx connection
	defer DisconnectInflux() // teardown influx connection

	measure := "test_write_01"
	field := "id"
	count := getCount(t, measure, field)

	tags := map[string]string{"test": t.Name()}
	err := Write(measure, map[string]interface{}{field: 42}, tags)
	if err != nil {
		t.Fatal("Failed to write to influx due to error:", err)
	}

	if getCountWhere(t, measure, field, "\"file\" = 'fluxlog_test.go'") <= count {
		t.Fatal("Failed to Write point to influxdb")
	}
}

func TestWritef(t *testing.T) {
	configure()              // setup influx connection
	defer DisconnectInflux() // teardown influx connection

	measure := "failed to do thing with id %d"
	field := "d1"
	count := getCount(t, measure, field)

	err := Writef(measure)
	if err == nil {
		t.Fatal("Failed to generate writef error when insufficient values are given")
	}

	if getCountWhere(t, measure, field, "\"file\" = 'fluxlog_test.go'") != count {
		t.Fatal("Sometime was written using Writef to influxdb when it was expected not too")
	}

	err = Writef(measure, 42)
	if err != nil {
		t.Fatal("Failed to write to influx due to error:", err)
	}

	if getCountWhere(t, measure, field, "\"file\" = 'fluxlog_test.go'") <= count {
		t.Fatal("Failed to Writef point to influxdb")
	}
}

func TestWhitelist(t *testing.T) {
	configure()              // setup influx connection
	defer DisconnectInflux() // teardown influx connection

	measureDeny := "test_write_02"
	measureAllow := "test_write_03"
	AddMeasurementToWhitelist(measureAllow)
	AddMeasurementToWhitelist(measureAllow + "1")
	defer ChangeMeasurementsWhitelist([]string{})
	field := "id"
	count := getCount(t, measureDeny, field)

	err := Write(measureDeny, map[string]interface{}{field: 42}, map[string]string{})
	if err == nil {
		t.Fatal("Failed to write to influx due to error:", err)
	}

	if getCountWhere(t, measureDeny, field, "\"file\" = 'fluxlog_test.go'") != count {
		t.Fatal("Something was written to influx when the measurement was not whitelisted")
	}

	err = Write(measureAllow, map[string]interface{}{field: 42}, map[string]string{})
	if err != nil {
		t.Fatal("Failed to write whitelisted measure to influx due to error:", err)
	}

	if getCountWhere(t, measureAllow, field, "\"file\" = 'fluxlog_test.go'") <= count {
		t.Fatal("Failed to write whitelisted measure to influx")
	}
}

func BenchmarkWrite(b *testing.B) {
	configure()              // setup influx connection
	defer DisconnectInflux() // teardown influx connection

	var err error
	fields := map[string]interface{}{"id": 42}
	tags := map[string]string{"test": b.Name()}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err = Write("test_write_04", fields, tags)
		if err != nil {
			b.Fatal("Write in benchmark failed with error:", err)
		}
	}
}

func BenchmarkWritef(b *testing.B) {
	configure()              // setup influx connection
	defer DisconnectInflux() // teardown influx connection

	var err error
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err = Writef("benchmarking thing with id %d", n)
		if err != nil {
			b.Fatal("Writef in benchmark failed with error:", err)
		}
	}
}

func BenchmarkWritefSlow(b *testing.B) {
	if testing.Short() {
		b.Skipf("Skipping %s in short mode", b.Name())
	}
	configure()              // setup influx connection
	defer DisconnectInflux() // teardown influx connection

	var err error
	for n := 0; n < b.N; n++ {
		err = Writef("benchmarking slow thing with id %d", n)
		if err != nil {
			b.Fatal("Writef in benchmark failed with error:", err)
		}
		time.Sleep(time.Duration(+rand.Intn(1000)) * time.Millisecond)
	}
}

func BenchmarkWriteReconnect(b *testing.B) {
	configure()              // setup influx connection
	defer DisconnectInflux() // teardown influx connection

	var err error
	fields := map[string]interface{}{"id": 42}
	tags := map[string]string{"test": b.Name()}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err = Write("test_write_05", fields, tags)
		if err != nil {
			b.Fatal("Write in benchmark failed with error:", err)
		}
		DisconnectInflux()
	}
}

func getCountWhere(t *testing.T, measurement string, field string, where string) int64 {
	t.Helper()
	res, err := queryInflux("SELECT count(" + field + ") FROM \"" + measurement + "\" WHERE " + where)
	if err != nil || res[0].Series == nil {
		return 0
	}
	val, err := res[0].Series[0].Values[0][1].(json.Number).Int64()
	if err != nil {
		return 0
	}
	return val
}

func getCount(t *testing.T, measurement string, field string) int64 {
	t.Helper()
	res, err := queryInflux("SELECT count(" + field + ") FROM \"" + measurement + "\" WHERE \"env\" ='test'")
	if err != nil || res[0].Series == nil {
		return 0
	}
	val, err := res[0].Series[0].Values[0][1].(json.Number).Int64()
	if err != nil {
		return 0
	}
	return val
}
