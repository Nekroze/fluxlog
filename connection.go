// Package provides a simple write only interface to influxdb for storing events instead of logs.
package fluxlog

import (
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
)

var client influx.Client
var db string = "fluxlog"
var precision string = "ms"
var username string
var password string
var address string

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
