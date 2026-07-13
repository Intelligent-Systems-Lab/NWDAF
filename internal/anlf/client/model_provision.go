package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/logger"
)

func (c *Client) SyncModelProvisionBinding(
	ctx context.Context,
	subscriptionID string,
	binding contract.ModelProvisionBinding,
) error {
	return c.sendJSON(
		ctx,
		http.MethodPut,
		c.subscriptionURL(subscriptionID, "/model-provision-binding"),
		binding,
		http.StatusNoContent,
		10*time.Second,
		"sync AnLF backend model provision binding",
	)
}

func (c *Client) ApplyModelProvisionEvent(
	ctx context.Context,
	event contract.ModelProvisionEvent,
) (*contract.ModelProvisionEventResponse, error) {
	if err := event.Validate(); err != nil {
		return nil, fmt.Errorf("apply model provision event: %w", err)
	}
	body, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal model provision event: %w", err)
	}
	requestCtx, cancel, err := timeoutContextFromParent(ctx, 120*time.Second, "apply model provision event")
	if err != nil {
		return nil, err
	}
	defer cancel()

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			if err = waitForRetry(requestCtx, time.Duration(attempt)*time.Second); err != nil {
				return nil, fmt.Errorf("apply model provision event: %w", err)
			}
		}
		result, retry, attemptErr := c.applyModelProvisionAttempt(requestCtx, body)
		if attemptErr == nil {
			return result, nil
		}
		lastErr = attemptErr
		if !retry {
			return nil, attemptErr
		}
	}
	return nil, lastErr
}

type modelProvisionErrorBody struct {
	Detail string `json:"detail"`
}

func (c *Client) applyModelProvisionAttempt(
	ctx context.Context,
	body []byte,
) (*contract.ModelProvisionEventResponse, bool, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		c.endpoint+"/model-provision-events",
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, false, fmt.Errorf("create model provision event request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, fmt.Errorf("send model provision event: %w", ctx.Err())
		}
		return nil, true, fmt.Errorf("send model provision event: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.AnlfLog.Debugf("failed to close model provision event response: %v", closeErr)
		}
	}()

	if resp.StatusCode == http.StatusOK {
		var result contract.ModelProvisionEventResponse
		if decodeErr := json.NewDecoder(resp.Body).Decode(&result); decodeErr != nil {
			return nil, false, fmt.Errorf("decode model provision event response: %w", decodeErr)
		}
		return &result, false, nil
	}

	responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if readErr != nil {
		return nil, false, fmt.Errorf(
			"apply model provision event: status=%d read response: %w",
			resp.StatusCode,
			readErr,
		)
	}
	detail := strings.TrimSpace(string(responseBody))
	var backendError modelProvisionErrorBody
	if json.Unmarshal(responseBody, &backendError) == nil && backendError.Detail != "" {
		detail = backendError.Detail
	}
	retry := resp.StatusCode >= http.StatusInternalServerError ||
		(resp.StatusCode == http.StatusConflict && detail == "STALE_RUNTIME_STATE")
	return nil, retry, fmt.Errorf(
		"apply model provision event: status=%d detail=%s",
		resp.StatusCode,
		detail,
	)
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
