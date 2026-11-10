package domain

import "time"

// Record is the envelope passed between pipeline stages.
// The payload is one of the source-specific types in this package.
type Record struct {
	ID         RecordID
	JobID      JobID
	SourceID   SourceID
	ObservedAt time.Time
	Payload    Payload
}

// Payload is the closed set of source records.
type Payload interface {
	sourceKind() SourceKind
}

// COVIDCase is one row from the COVID-19 CSV source.
type COVIDCase struct {
	Date   string
	Region string
	Cases  int64
	Deaths int64
}

func (COVIDCase) sourceKind() SourceKind { return SourceCSV }

// CryptoPrice is one price observation from the JSON API source.
type CryptoPrice struct {
	Symbol string
	Price  float64
	At     time.Time
}

func (CryptoPrice) sourceKind() SourceKind { return SourceJSON }

// WeatherObservation is one reading from the weather HTTP source.
type WeatherObservation struct {
	Latitude  float64
	Longitude float64
	TempC     float64
	At        time.Time
}

func (WeatherObservation) sourceKind() SourceKind { return SourceHTTP }

// SourceKindOf returns the kind carried by payload.
func SourceKindOf(payload Payload) SourceKind {
	if payload == nil {
		return ""
	}
	return payload.sourceKind()
}
