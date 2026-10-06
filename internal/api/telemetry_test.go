package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func postTelemetry(t *testing.T, server *Server, body string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/repeaters/R07/telemetry", bytes.NewBufferString(body))
	server.Handler().ServeHTTP(recorder, request)
	return recorder
}

func TestTelemetrySequenceConflictKeepsFirstReading(t *testing.T) {
	server := testServer(t)

	first := postTelemetry(t, server, `{"kind":"temperature","value":42,"unit":"C","sequence":1,"generation":1}`)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first sample status=%d body=%s", first.Code, first.Body.String())
	}

	// Identical redelivery is idempotent and must not error or duplicate.
	again := postTelemetry(t, server, `{"kind":"temperature","value":42,"unit":"C","sequence":1,"generation":1}`)
	if again.Code != http.StatusAccepted {
		t.Fatalf("idempotent redelivery status=%d body=%s", again.Code, again.Body.String())
	}

	// Same sequence, different value is a conflict: 409, first fact retained.
	conflict := postTelemetry(t, server, `{"kind":"temperature","value":99,"unit":"C","sequence":1,"generation":1}`)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflicting sample status=%d body=%s", conflict.Code, conflict.Body.String())
	}

	history := httptest.NewRecorder()
	server.Handler().ServeHTTP(history, httptest.NewRequest(http.MethodGet, "/api/repeaters/R07/telemetry", nil))
	if history.Code != http.StatusOK {
		t.Fatalf("history status=%d body=%s", history.Code, history.Body.String())
	}
	var payload struct {
		Samples []struct {
			Value float64 `json:"value"`
		} `json:"samples"`
	}
	if err := json.Unmarshal(history.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Samples) != 1 || payload.Samples[0].Value != 42 {
		t.Fatalf("field reading must remain the first fact: %+v", payload.Samples)
	}
}

func TestTelemetrySequenceConflictDoesNotRetrip(t *testing.T) {
	server := testServer(t)

	trip := postTelemetry(t, server, `{"kind":"temperature","value":90,"unit":"C","sequence":1,"generation":1}`)
	if trip.Code != http.StatusAccepted {
		t.Fatalf("tripping sample status=%d body=%s", trip.Code, trip.Body.String())
	}
	replay := postTelemetry(t, server, `{"kind":"temperature","value":95,"unit":"C","sequence":1,"generation":1}`)
	if replay.Code != http.StatusConflict {
		t.Fatalf("conflicting replay status=%d body=%s", replay.Code, replay.Body.String())
	}

	repeater := httptest.NewRecorder()
	server.Handler().ServeHTTP(repeater, httptest.NewRequest(http.MethodGet, "/api/repeaters/R07", nil))
	if repeater.Code != http.StatusOK {
		t.Fatalf("repeater status=%d body=%s", repeater.Code, repeater.Body.String())
	}
	var snapshot struct {
		Health       string  `json:"health"`
		TemperatureC float64 `json:"temperature_c"`
	}
	if err := json.Unmarshal(repeater.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Health != "latched-trip" || snapshot.TemperatureC != 90 {
		t.Fatalf("conflicting replay must not overwrite the first field reading: %+v", snapshot)
	}
}
