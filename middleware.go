package fluxlog

import (
	"net/http"
	"time"
)

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{w, http.StatusOK}
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// This struct provides an interface compatible with http.Handler and will
// storage data in influxdb about each request that goes through.
type Middleware struct {
	http.Handler
	Measurement string
	Tags        map[string]string
}

func (m *Middleware) buildTags(inputs map[string]string) map[string]string {
	for k, v := range m.Tags {
		inputs[k] = v
	}
	return inputs
}

func (m *Middleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// pre
	start := time.Now()
	rw := newResponseWriter(w)
	// do
	m.Handler.ServeHTTP(rw, r)
	// post
	elapsed := time.Since(start)
	fields := map[string]interface{}{
		"milliseconds": int64(elapsed / time.Millisecond),
		"status":       rw.statusCode,
	}
	tags := m.buildTags(map[string]string{
		"method": r.Method,
		"uri":    r.URL.RequestURI(),
		"host":   r.Host,
	})

	Write(m.Measurement, fields, tags)
}
