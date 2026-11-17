package http

import (
	"context"
	"encoding/json"
	"fmt"
	stdhttp "net/http"
	"time"

	"github.com/sambit-srcm/data-processing-pipeline/internal/domain"
)

type weatherResponse struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Current   struct {
		Temperature float64 `json:"temperature_2m"`
		Time        string  `json:"time"`
	} `json:"current"`
}

// Client reads weather observations from an Open-Meteo-style URL.
type Client struct {
	HTTP    *stdhttp.Client
	Timeout time.Duration
}

// Fetch loads one weather observation from url.
func (c Client) Fetch(ctx context.Context, url string, jobID domain.JobID, sourceID domain.SourceID) ([]domain.Record, error) {
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = &stdhttp.Client{Timeout: timeout}
	}

	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	res, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != stdhttp.StatusOK {
		return nil, fmt.Errorf("weather http %d", res.StatusCode)
	}

	var body weatherResponse
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, err
	}
	at := time.Now().UTC()
	if body.Current.Time != "" {
		if parsed, err := time.Parse("2006-01-02T15:04", body.Current.Time); err == nil {
			at = parsed.UTC()
		}
	}
	return []domain.Record{{
		ID:         domain.RecordID(fmt.Sprintf("%s-1", sourceID)),
		JobID:      jobID,
		SourceID:   sourceID,
		ObservedAt: at,
		Payload: domain.WeatherObservation{
			Latitude:  body.Latitude,
			Longitude: body.Longitude,
			TempC:     body.Current.Temperature,
			At:        at,
		},
	}}, nil
}
