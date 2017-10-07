// Package provides a simple write only interface to influxdb for storing events instead of logs.
package fluxlog

import (
	"fmt"
	"path"
	"regexp"
	"runtime"
	"strings"
)

var GlobalTags map[string]string
var MeasurementWhitelist []string

// Switch to save metadata (calling file and line number) when saving an event.
var Metadata bool = false
var writefRegex *regexp.Regexp

func init() {
	writefRegex = regexp.MustCompile("%(#|\\+)?([a-zA-Z])")
}

func SaveMetadata(new bool) {
	Metadata = new
}

// Add a measurement to the whitelist. In the case of writef this is the unformatted string.
// If nothing has been whitelisted it acts as though all measurements are allowed.
// This allows avoiding costly mistakes by writing to a measurement name with a typo.
func AddMeasurementToWhitelist(measurement string) {
	if MeasurementWhitelist == nil {
		MeasurementWhitelist = []string{measurement}
		return
	}
	MeasurementWhitelist = append(MeasurementWhitelist, measurement)
}

func measurementWhitelisted(measurement string) bool {
	if MeasurementWhitelist == nil || len(MeasurementWhitelist) == 0 {
		return true
	}
	for _, allowed := range MeasurementWhitelist {
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
	return enqueue(measurement, mergeFields(getMetadataFields(2), fields), mergeTags(GlobalTags, itags))
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
	return enqueue(measurement, mergeFields(getMetadataFields(2), fieldMap), GlobalTags)
}

func mergeTags(left map[string]string, right map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range left {
		out[k] = v
	}
	for k, v := range right {
		out[k] = v
	}
	return out
}

func mergeFields(left map[string]interface{}, right map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{}
	for k, v := range left {
		out[k] = v
	}
	for k, v := range right {
		out[k] = v
	}
	return out
}

func getMetadataFields(skip int) map[string]interface{} {
	if !Metadata {
		return map[string]interface{}{}
	}
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
