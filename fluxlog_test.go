package fluxlog

import (
	"encoding/json"
	"testing"
)

var address string = "http://storage:8086"

func TestConnectInfluxTcp(t *testing.T) {
	connectt(t)
	ChangeGlobalTags(map[string]string{"env": "test"})
	ChangePrecision("us")
	SaveMetadata(true)
	defer DisconnectInflux()
}

func TestWrite(t *testing.T) {
	connectt(t)              // setup influx connection
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
	connectt(t)              // setup influx connection
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
	connectt(t)              // setup influx connection
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
	connectb(b)              // setup influx connection
	defer DisconnectInflux() // teardown influx connection

	var err error
	fields := map[string]interface{}{"id": 42}
	tags := map[string]string{"test": b.Name()}
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		err = Write("test_write_03", fields, tags)
	}
	if err != nil {
		b.Fatal("Final write in benchmark failed with error:", err)
	}
}

func BenchmarkWritef(b *testing.B) {
	connectb(b)              // setup influx connection
	defer DisconnectInflux() // teardown influx connection

	var err error
	for n := 0; n < b.N; n++ {
		err = Writef("benchmarking thing with id %d", n)
	}
	if err != nil {
		b.Fatal("Final writef in benchmark failed with error:", err)
	}
}

func connectb(b *testing.B) {
	b.Helper()
	err := connect()
	if err != nil {
		b.Fatal("Could not connect influx tcp with address", address)
	}
}

func connect() error {
	return ConnectInflux(address, "", "")
}

func connectt(t *testing.T) {
	t.Helper()
	err := connect()
	if err != nil {
		t.Fatal("Could not connect influx tcp with address", address)
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
