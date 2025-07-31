package snowapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/vjain20/gosnowapi/internal/auth"
)

// Config holds config needed to initialize the client.
type Config struct {
	Account     string
	User        string
	Role        string
	Database    string
	Schema      string
	Warehouse   string
	PrivateKey  []byte // PEM (PKCS8)
	PublicKey   []byte // PEM
	ExpireAfter time.Duration
	HTTPTimeout time.Duration
}

// Client is the main Snowflake SQL API client.
type Client struct {
	baseURL    string
	httpClient *http.Client
	config     Config
}

// NewClient initializes the client with config and default timeout.
func NewClient(cfg Config) (*Client, error) {
	if cfg.Account == "" || cfg.User == "" {
		return nil, fmt.Errorf("account and user are required")
	}

	timeout := cfg.HTTPTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	return &Client{
		baseURL:    fmt.Sprintf("https://%s.snowflakecomputing.com/api/v2/statements", cfg.Account),
		httpClient: &http.Client{Timeout: timeout},
		config:     cfg,
	}, nil
}

func (c *Client) authToken() (string, error) {
	return auth.GenerateJWT(auth.TokenConfig{
		Account:     c.config.Account,
		User:        c.config.User,
		PrivateKey:  c.config.PrivateKey,
		PublicKey:   c.config.PublicKey,
		ExpireAfter: c.config.ExpireAfter,
	})
}

func (c *Client) Query(statement string) ([][]any, error) {
	reqID := uuid.New().String()
	opts := &RequestOptions{
		RequestID: reqID,
	}

	resp, err := c.Execute(statement, false, opts)
	if err != nil {
		return nil, err
	}

	return resp.Data, nil
}

func (c *Client) Execute(statement string, async bool, opts *RequestOptions) (*QueryResponse, error) {
	// Prepare query payload
	body := QueryRequest{
		Statement: statement,
		Timeout:   60,
		ResultSetMetaData: &ResultSetMetaConfig{
			Format: "json", // Or "jsonv2"
		},
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Build URL with query params
	queryParams := url.Values{}
	queryParams.Set("async", strconv.FormatBool(async))
	queryParams.Set("nullable", "true")

	if opts != nil && opts.RequestID != "" {
		queryParams.Set("requestId", opts.RequestID)

		// Default retry to true if not explicitly false
		if opts.Retry == nil || *opts.Retry {
			queryParams.Set("retry", "true")
		}
	}

	fullURL := fmt.Sprintf("%s?%s", c.baseURL, queryParams.Encode())

	// Create request
	req, err := http.NewRequest("POST", fullURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set headers
	token, err := c.authToken()
	if err != nil {
		return nil, fmt.Errorf("failed to generate auth token: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Send request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Decode response
	var result QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Check for async status
	if resp.StatusCode == http.StatusAccepted || result.Code == "333334" {
		// Async execution in progress, return handle
		return &result, nil
	}

	// Handle unexpected errors
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API error: %s (code %s)", result.Message, result.Code)
	}

	return &result, nil
}
