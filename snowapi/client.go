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

// Poll checks the status of an asynchronous query or fetches a partition of results.
// Returns the parsed response, HTTP status code, and error if any.
func (c *Client) Poll(handle string, partition int) (*QueryResponse, int, error) {
	endpoint := fmt.Sprintf("%s/%s", c.baseURL, handle)

	// Add partition query param if needed
	if partition > 0 {
		endpoint = fmt.Sprintf("%s?partition=%d", endpoint, partition)
	}

	// Generate auth token
	token, err := c.authToken()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to generate auth token: %w", err)
	}

	// Build request
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create poll request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Send request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("poll request failed: %w", err)
	}
	defer resp.Body.Close()

	// Parse response
	var result QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("failed to decode poll response: %w", err)
	}

	return &result, resp.StatusCode, nil
}

func (c *Client) Cancel(statementHandle string) error {
	// Generate auth token
	token, err := c.authToken()
	if err != nil {
		return fmt.Errorf("failed to generate auth token: %w", err)
	}

	// Build URL
	cancelURL := fmt.Sprintf("%s/%s/cancel", c.baseURL, statementHandle)

	// Create POST request with empty JSON body
	req, err := http.NewRequest("POST", cancelURL, bytes.NewReader([]byte("{}")))
	if err != nil {
		return fmt.Errorf("failed to create cancel request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Send request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("cancel request failed: %w", err)
	}
	defer resp.Body.Close()

	// Handle non-200s
	if resp.StatusCode != http.StatusOK {
		var errResp QueryErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
			return fmt.Errorf("cancel failed with status %d", resp.StatusCode)
		}
		return fmt.Errorf("cancel failed: %s (code %s)", errResp.Message, errResp.Code)
	}

	return nil
}

// WaitUntilComplete polls until the statement finishes execution or fails.
// Returns the final result or an error.
func (c *Client) WaitUntilComplete(handle string, interval time.Duration, maxRetries int) (*QueryResponse, error) {
	for i := 0; i < maxRetries; i++ {
		resp, status, err := c.Poll(handle, 0)
		if err != nil {
			return nil, err
		}

		switch status {
		case http.StatusOK:
			return resp, nil // success
		case http.StatusAccepted:
			time.Sleep(interval) // still running
		case http.StatusUnprocessableEntity:
			return nil, fmt.Errorf("query execution failed: %s (code %s)", resp.Message, resp.Code)
		default:
			return nil, fmt.Errorf("unexpected status %d: %s", status, resp.Message)
		}
	}

	return nil, fmt.Errorf("max retries exceeded while waiting for completion")
}
