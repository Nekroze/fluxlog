package fluxlog

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMiddleware(t *testing.T) {
	configure()              // setup influx connection
	defer DisconnectInflux() // teardown influx connection

	testcheck := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"alive": true}`)
	}
	measure := "endpoints-test"
	testhandler := http.HandlerFunc(testcheck)
	middleware := Middleware{testhandler, measure, map[string]string{"env": "test"}}

	req, err := http.NewRequest("GET", "/test", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()

	count := getCountWhere(t, measure, "status", "\"uri\" = '/test'")

	middleware.ServeHTTP(rr, req)

	if getCountWhere(t, measure, "status", "\"uri\" = '/test'") <= count {
		t.Fatal("Failed to Write point to influxdb")
	}

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}
}

func BenchmarkMiddleware(b *testing.B) {
	configure()              // setup influx connection
	defer DisconnectInflux() // teardown influx connection

	testcheck := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"alive": true}`)
	}
	measure := "endpoints-benchmark"
	testhandler := http.HandlerFunc(testcheck)
	middleware := Middleware{testhandler, measure, map[string]string{"env": "test"}}

	req, err := http.NewRequest("GET", "/benchmark", nil)
	if err != nil {
		b.Fatal(err)
	}
	rr := httptest.NewRecorder()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		middleware.ServeHTTP(rr, req)
	}
}
