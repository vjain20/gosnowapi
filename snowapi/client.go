package snowapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

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
	resp, err := c.Execute(statement, false)
	if err != nil {
		return nil, err
	}
	return resp.Data, nil
}

// Execute runs a SQL statement using the Snowflake SQL API.
func (c *Client) Execute(statement string, async bool) (*QueryResponse, error) {
	// Build the request body
	body := QueryRequest{
		Statement: statement,
		Timeout:   60,
		ResultSetMetaData: &ResultSetMetaConfig{
			Format: "json", // or "jsonv2"
		},
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Build the full URL with query parameters
	url := fmt.Sprintf("%s?async=%t&nullable=true", c.baseURL, async)

	// Create HTTP request
	req, err := http.NewRequest("POST", url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}
	token, err := c.authToken()
	if err != nil {
		return nil, fmt.Errorf("failed to generate auth token: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Send the request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Handle error or partial responses
	if resp.StatusCode != http.StatusOK {
		var errResp QueryErrorResponse
		json.NewDecoder(resp.Body).Decode(&errResp)
		return nil, fmt.Errorf("API error: %s (code %s)", errResp.Message, errResp.Code)
	}

	// Decode success response
	var result QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}
