package fluxlog

import (
	"fmt"

	influx "github.com/influxdata/influxdb/client/v2"
)

var client influx.Client

// Change the database events will be written to.
var DB string = "fluxlog"

// Change the precision used when writing event timestamps. eg, "s" or "us"
var Precision string = "ms"

// Username for connecting to influxdb. Storing this globally allows for self recovering connections.
var Username string

// Password for connecting to influxdb. Storing this globally allows for self recovering connections.
var Password string

// Address for connecting to influxdb. Storing this globally allows for self recovering connections.
var Address string

// Disconnect fluxlog by removing the inluxdb client connection.
func DisconnectInflux() {
	client = nil
}

// Connect to influxdb over http using global credentials.
// This will be called automatically on write if there is no open influx connection.
func ConnectInflux() error {
	if client != nil {
		return nil
	} else if len(Address) == 0 {
		return fmt.Errorf("no influxdb address provided for connection")
	}
	c, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr:     Address,
		Username: Username,
		Password: Password,
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
	_, err := queryInflux(fmt.Sprintf("CREATE DATABASE %s", DB))
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
		Database: DB,
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
