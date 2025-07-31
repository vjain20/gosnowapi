package snowapi

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

// mockPoller defines the signature for a mock Poll function.
type mockPoller func(handle string, partition int) (*QueryResponse, int, error)

// mockClient is a Client with a mock Poll method.
type mockClient struct {
	*Client
	pollFunc mockPoller
}

// Override WaitUntilComplete to use the mock pollFunc.
func (m *mockClient) WaitUntilComplete(handle string, interval time.Duration, maxRetries int) (*QueryResponse, error) {
	for i := 0; i < maxRetries; i++ {
		resp, status, err := m.pollFunc(handle, 0)
		if err != nil {
			return nil, err
		}

		switch status {
		case http.StatusOK:
			return resp, nil
		case http.StatusAccepted:
			time.Sleep(interval)
		case http.StatusUnprocessableEntity:
			return nil, errors.New("query execution failed: " + resp.Message + " (code " + resp.Code + ")")
		default:
			return nil, errors.New("unexpected status " + http.StatusText(status) + ": " + resp.Message)
		}
	}
	return nil, errors.New("max retries exceeded while waiting for completion")
}

func TestWaitUntilComplete_Success(t *testing.T) {
	mockResp := &QueryResponse{
		Code:    "090001",
		Message: "successfully executed",
		Data:    [][]any{{"row1"}, {"row2"}},
	}

	client := &mockClient{pollFunc: func(handle string, partition int) (*QueryResponse, int, error) {
		return mockResp, http.StatusOK, nil
	}}

	resp, err := client.WaitUntilComplete("test-handle", 10*time.Millisecond, 3)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if resp.Message != "successfully executed" {
		t.Errorf("unexpected message: %s", resp.Message)
	}
}

func TestWaitUntilComplete_RetryAndSuccess(t *testing.T) {
	calls := 0
	client := &mockClient{pollFunc: func(handle string, partition int) (*QueryResponse, int, error) {
		calls++
		if calls < 2 {
			return &QueryResponse{Message: "still processing"}, http.StatusAccepted, nil
		}
		return &QueryResponse{Message: "done"}, http.StatusOK, nil
	}}

	resp, err := client.WaitUntilComplete("test-handle", 10*time.Millisecond, 3)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if resp.Message != "done" {
		t.Errorf("unexpected message: %s", resp.Message)
	}
}

func TestWaitUntilComplete_MaxRetriesExceeded(t *testing.T) {
	client := &mockClient{pollFunc: func(handle string, partition int) (*QueryResponse, int, error) {
		return &QueryResponse{Message: "still running"}, http.StatusAccepted, nil
	}}

	_, err := client.WaitUntilComplete("test-handle", 10*time.Millisecond, 2)
	if err == nil || err.Error() != "max retries exceeded while waiting for completion" {
		t.Errorf("expected max retries error, got: %v", err)
	}
}

func TestWaitUntilComplete_ErrorResponse(t *testing.T) {
	client := &mockClient{pollFunc: func(handle string, partition int) (*QueryResponse, int, error) {
		return &QueryResponse{Message: "execution error", Code: "422"}, http.StatusUnprocessableEntity, nil
	}}

	_, err := client.WaitUntilComplete("test-handle", 10*time.Millisecond, 1)
	if err == nil || err.Error() != "query execution failed: execution error (code 422)" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestWaitUntilComplete_HTTPError(t *testing.T) {
	client := &mockClient{pollFunc: func(handle string, partition int) (*QueryResponse, int, error) {
		return nil, 0, errors.New("network error")
	}}

	_, err := client.WaitUntilComplete("test-handle", 10*time.Millisecond, 1)
	if err == nil || err.Error() != "network error" {
		t.Errorf("unexpected error: %v", err)
	}
}
