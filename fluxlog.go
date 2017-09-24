// Package provides a simple write only interface to influxdb for storing events instead of logs.
package fluxlog

import (
	"fmt"
	influx "github.com/influxdata/influxdb/client/v2"
	"regexp"
	"strings"
	"time"
)

// Influxdb client connection.
var client influx.Client

// Influxdb database to write too.
var db string = "fluxlog"

// Global tags that are always used when sending an event but may be overridden per request.
var tags map[string]string

// Measurements that are allowed to be used, if empty it acts as though all measurements are allowed.
// This allows avoiding costly mistakes by writing to a measurement name with a typo.
var measurementWhitelist []string

// Precision to use when storing events. eg, "s" or "us"
var precision string = "s"
var writefRegex *regexp.Regexp

func init() {
	writefRegex = regexp.MustCompile("%(#|\\+)?([a-zA-Z])")
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
func AddMeasurementsToWhitelist(measurements []string) {
	for _, m := range measurements {
		AddMeasurementToWhitelist(m)
	}
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
func ConnectInflux(addr string, user string, pass string) error {
	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr:     addr,
		Username: user,
		Password: pass,
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
	if client == nil {
		return fmt.Errorf("influxdb client is not connected")
	} else if measurementWhitelisted(measurement) == false {
		return fmt.Errorf("measurement %s not in whitelist", measurement)
	}

	bp, err := influx.NewBatchPoints(influx.BatchPointsConfig{
		Database:  db,
		Precision: precision,
	})
	if err != nil {
		return err
	}

	wtags := GetGlobalTags()
	for k, v := range itags {
		wtags[k] = v
	}

	pt, err := influx.NewPoint(measurement, wtags, fields, time.Now())
	if err != nil {
		return err
	}
	bp.AddPoint(pt)

	err = client.Write(bp)
	return err
}

func ensureSchema() error {
	_, err := queryInflux(fmt.Sprintf("CREATE DATABASE %s", db))
	return err
}

func queryInflux(cmd string) (res []influx.Result, err error) {
	if client == nil {
		return nil, fmt.Errorf("influxdb client is not connected")
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

// Write an event to influxdb using a similar call signature to logging a message
func Writef(signature string, fields ...interface{}) error {
	if client == nil {
		return fmt.Errorf("influxdb client is not connected")
	}
	fieldMap := make(map[string]interface{})
	fieldMapCounter := make(map[string]int)
	var suffix int
	for i, field := range writefRegex.FindAllString(signature, -1) {
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
	return Write(signature, fieldMap, map[string]string{})
}
