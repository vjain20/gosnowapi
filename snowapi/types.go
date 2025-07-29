package snowapi

// QueryRequest represents the request body for executing a SQL statement.
type QueryRequest struct {
	Statement         string               `json:"statement"`
	Timeout           int                  `json:"timeout,omitempty"`
	ResultSetMetaData *ResultSetMetaConfig `json:"resultSetMetaData,omitempty"`
	// Future options: Async, RequestID, etc.
}

// ResultSetMetaConfig defines the format of metadata in response.
type ResultSetMetaConfig struct {
	Format string `json:"format"` // "json" or "jsonv2"
}

// QueryResponse represents a successful execution of a SQL statement.
type QueryResponse struct {
	ResultSetMetaData  ResultSetMetaData `json:"resultSetMetaData"`
	Data               [][]interface{}   `json:"data"`
	Code               string            `json:"code"`
	StatementStatusURL string            `json:"statementStatusUrl"`
	StatementHandle    string            `json:"statementHandle"`
	SQLState           string            `json:"sqlState"`
	Message            string            `json:"message"`
	CreatedOn          int64             `json:"createdOn"`
}

// ResultSetMetaData describes the metadata for returned data.
type ResultSetMetaData struct {
	NumRows       int             `json:"numRows"`
	Format        string          `json:"format"`
	RowType       []ColumnMeta    `json:"rowType"`
	PartitionInfo []PartitionMeta `json:"partitionInfo"`
}

// ColumnMeta describes a single column in the result set.
type ColumnMeta struct {
	Name       string  `json:"name"`
	Database   string  `json:"database"`
	Schema     string  `json:"schema"`
	Table      string  `json:"table"`
	Nullable   bool    `json:"nullable"`
	Scale      *int    `json:"scale"`
	ByteLength *int    `json:"byteLength"`
	Length     *int    `json:"length"`
	Type       string  `json:"type"`
	Precision  *int    `json:"precision"`
	Collation  *string `json:"collation"`
}

// PartitionMeta provides partition-level metadata (when results are paginated).
type PartitionMeta struct {
	RowCount         int  `json:"rowCount"`
	UncompressedSize int  `json:"uncompressedSize"`
	CompressedSize   *int `json:"compressedSize,omitempty"`
}

// QueryErrorResponse captures error payloads (e.g. 422, 408)
type QueryErrorResponse struct {
	Code            string `json:"code"`
	Message         string `json:"message"`
	SQLState        string `json:"sqlState,omitempty"`
	StatementHandle string `json:"statementHandle,omitempty"`
}
