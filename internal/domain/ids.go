package domain

// JobID identifies one pipeline run.
type JobID string

// SourceID identifies one configured input.
type SourceID string

// RecordID identifies one record within a source.
type RecordID string

// SourceKind is the kind of input a record came from.
type SourceKind string

// Supported source kinds.
const (
	SourceCSV  SourceKind = "csv"
	SourceJSON SourceKind = "json"
	SourceHTTP SourceKind = "http"
)

// Stage is a pipeline stage that can report an error.
type Stage string

// Pipeline stages that can report errors.
const (
	StageIngestion      Stage = "ingestion"
	StageValidation     Stage = "validation"
	StageTransformation Stage = "transformation"
	StageAggregation    Stage = "aggregation"
	StageExport         Stage = "export"
)

// ExportKind is a configured output target.
type ExportKind string

// Supported export targets.
const (
	ExportCSV    ExportKind = "csv"
	ExportJSON   ExportKind = "json"
	ExportSQLite ExportKind = "sqlite"
)
