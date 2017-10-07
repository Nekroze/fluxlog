// Package provides a simple write only interface to influxdb for storing events instead of logs.
package fluxlog

import (
	"fmt"
	"path"
	"regexp"
	"runtime"
	"strings"
	"time"

	influx "github.com/influxdata/influxdb/client/v2"
)

var client influx.Client
var db string = "fluxlog"
var tags map[string]string
var measurementWhitelist []string
var precision string = "ms"
var metadata bool = false
var username string
var password string
var address string
var writefRegex *regexp.Regexp

func init() {
	writefRegex = regexp.MustCompile("%(#|\\+)?([a-zA-Z])")
}

// Address for connecting to influxdb. Storing this globally allows for self recovering connections.
func SetAddress(new string) {
	address = new
}

// Username for connecting to influxdb. Storing this globally allows for self recovering connections.
func SetUsername(new string) {
	username = new
}

// Password for connecting to influxdb. Storing this globally allows for self recovering connections.
func SetPassword(new string) {
	password = new
}

// Switch to save metadata (calling file and line number) when saving an event.
func SaveMetadata(new bool) {
	metadata = new
}

// Change the database events will be written to.
func ChangeDatabase(name string) {
	if len(name) > 0 {
		db = name
		if client != nil {
			ensureSchema()
		}
	}
}

// Change the precision used when writing event timestamps. eg, "s" or "us"
func ChangePrecision(new string) {
	precision = new
}

// Add a measurement to the whitelist. In the case of writef this is the unformatted string.
// If nothing has been whitelisted it acts as though all measurements are allowed.
// This allows avoiding costly mistakes by writing to a measurement name with a typo.
func AddMeasurementToWhitelist(measurement string) {
	if measurementWhitelist == nil {
		measurementWhitelist = []string{measurement}
		return
	}
	measurementWhitelist = append(measurementWhitelist, measurement)
}

// Add multiple measurements to the whitelist. See AddMeasurementToWhitelist
func ChangeMeasurementsWhitelist(measurements []string) {
	measurementWhitelist = measurements
}

// Change global tags that are always used when sending an event but may be overridden per request.
func ChangeGlobalTags(newtags map[string]string) {
	tags = newtags
}

// Get the current global tags.
func GetGlobalTags() map[string]string {
	if tags != nil {
		return tags
	}
	return map[string]string{}
}

// Disconnect fluxlog by removing the inluxdb client connection.
func DisconnectInflux() {
	client = nil
}

// Connect to influxdb over http using the given address and credentials.
// Credentials may both be empty strings for no authentication.
// This will be called automatically on write if there is no open influx connection
func ConnectInflux() error {
	if client != nil {
		return nil
	} else if len(address) == 0 {
		return fmt.Errorf("no influxdb address provided for connection.")
	}
	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr:     address,
		Username: username,
		Password: password,
	})
	if err == nil {
		client = c
		err = ensureSchema()
		if err != nil {
			DisconnectInflux()
		}
	}
	return err
}

func measurementWhitelisted(measurement string) bool {
	if measurementWhitelist == nil || len(measurementWhitelist) == 0 {
		return true
	}
	for _, allowed := range measurementWhitelist {
		if measurement == allowed {
			return true
		}
	}
	return false
}

// Write an event to influxdb.
func Write(measurement string, fields map[string]interface{}, itags map[string]string) error {
	if measurementWhitelisted(measurement) == false {
		return fmt.Errorf("measurement %s not in whitelist", measurement)
	}
	return write(measurement, fields, itags)
}

func write(measurement string, fields map[string]interface{}, itags map[string]string) error {
	wtags := map[string]string{}
	for k, v := range GetGlobalTags() {
		wtags[k] = v
	}
	for k, v := range itags {
		wtags[k] = v
	}

	if metadata {
		fields = mergeFields(getMetadataFields(3), fields)
	}
	pt, err := influx.NewPoint(measurement, wtags, fields, time.Now())
	if err != nil {
		return err
	}
	queue <- pt
	return nil
}

// Write an event to influxdb using a similar call signature to logging a message
func Writef(measurement string, fields ...interface{}) error {
	if measurementWhitelisted(measurement) == false {
		return fmt.Errorf("measurement %s not in whitelist", measurement)
	}

	fieldMap := make(map[string]interface{})
	fieldMapCounter := make(map[string]int)
	var suffix int
	for i, field := range writefRegex.FindAllString(measurement, -1) {
		suffix = 1
		for k, v := range fieldMapCounter {
			if k == field {
				suffix = v + 1
			}
		}
		if i >= len(fields) {
			return fmt.Errorf("Insufficient number of fields provided")
		}
		fieldMap[fmt.Sprintf("%s%d", strings.TrimLeft(field, "%"), suffix)] = fields[i]
	}
	if metadata {
		fieldMap = mergeFields(getMetadataFields(2), fieldMap)
	}
	return write(measurement, fieldMap, map[string]string{})
}

func mergeFields(left map[string]interface{}, right map[string]interface{}) map[string]interface{} {
	for k, v := range right {
		left[k] = v
	}
	return left
}

func getMetadataFields(skip int) map[string]interface{} {
	pc, fun, err := getRunFunc(skip + 1)
	if err != nil {
		return map[string]interface{}{}
	}
	file, line := fun.FileLine(pc)
	return map[string]interface{}{
		"func": fun.Name(),
		"file": path.Base(file),
		"line": line,
	}
}

func getRunFunc(skip int) (uintptr, *runtime.Func, error) {
	fpcs := make([]uintptr, 1)
	n := runtime.Callers(skip+1, fpcs)
	if n == 0 {
		return 0, nil, fmt.Errorf("Could not retriever caller runtime info")
	}

	fun := runtime.FuncForPC(fpcs[0] - 1)
	if fun == nil {
		return 0, nil, fmt.Errorf("Could not retriever caller runtime info")
	}
	return fpcs[0] - 1, fun, nil
}

func ensureSchema() error {
	_, err := queryInflux(fmt.Sprintf("CREATE DATABASE %s", db))
	return err
}

func queryInflux(cmd string) (res []influx.Result, err error) {
	ProcessQueue()
	err = ConnectInflux()
	if err != nil {
		return nil, fmt.Errorf("influxdb client is not connected due to error: %s", err)
	}
	q := influx.Query{
		Command:  cmd,
		Database: db,
	}
	if response, err := client.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}
