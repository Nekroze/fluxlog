package fluxlog

import (
	"encoding/json"
	"testing"
)

var address string = "http://storage:8086"

func TestConnectInfluxTcp(t *testing.T) {
	connect(t)
	ChangeGlobalTags(map[string]string{"env": "test"})
	ChangePrecision("us")
	defer DisconnectInflux()
}

func TestWrite(t *testing.T) {
	connect(t)               // setup influx connection
	defer DisconnectInflux() // teardown influx connection

	measure := "test_write_01"
	field := "id"
	count := getCount(t, measure, field)

	tags := map[string]string{"test": t.Name()}
	err := Write(measure, map[string]interface{}{field: 42}, tags)
	if err != nil {
		t.Fatal("Failed to write to influx due to error:", err)
	}

	if getCount(t, measure, field) <= count {
		t.Fatal("Failed to Write point to influxdb")
	}
}

func TestWritef(t *testing.T) {
	connect(t)               // setup influx connection
	defer DisconnectInflux() // teardown influx connection

	measure := "failed to do thing with id %d"
	field := "d1"
	count := getCount(t, measure, field)

	err := Writef(measure, 42)
	if err != nil {
		t.Fatal("Failed to write to influx due to error:", err)
	}

	if getCount(t, measure, field) <= count {
		t.Fatal("Failed to Writef point to influxdb")
	}
}

func connect(t *testing.T) {
	t.Helper()
	err := ConnectInflux(address, "", "")
	if err != nil {
		t.Fatal("Could not connect influx tcp with address", address)
	}
}

func getCount(t *testing.T, measurement string, field string) int64 {
	t.Helper()
	res, err := queryInflux("SELECT count(" + field + ") FROM \"" + measurement + "\"")
	if err != nil || res[0].Series == nil {
		return 0
	}
	val, err := res[0].Series[0].Values[0][1].(json.Number).Int64()
	if err != nil {
		return 0
	}
	return val
}
