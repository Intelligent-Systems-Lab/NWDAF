package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
	"github.com/free5gc/nwdaf/internal/logger"
)

func (c *Client) ApplySubscriptionRuntime(
	ctx context.Context,
	request contract.ApplySubscriptionRuntimeRequest,
) (*contract.ApplySubscriptionRuntimeResponse, error) {
	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal AnLF runtime apply request: %w", err)
	}

	requestURL := c.subscriptionURL(request.Subscription.SubscriptionID, "/runtime")
	ctx, cancel, err := timeoutContextFromParent(ctx, 120*time.Second, "AnLF backend runtime apply")
	if err != nil {
		return nil, err
	}
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, requestURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("create AnLF runtime apply request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send AnLF runtime apply request: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.AnlfLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("AnLF backend runtime apply failed: status=%d", resp.StatusCode)
	}

	var response contract.ApplySubscriptionRuntimeResponse
	if decodeErr := json.NewDecoder(resp.Body).Decode(&response); decodeErr != nil {
		return nil, fmt.Errorf("decode AnLF runtime apply response: %w", decodeErr)
	}

	return &response, nil
}

func (c *Client) ReleaseSubscriptionRuntime(ctx context.Context, subscriptionID string) error {
	ctx, cancel, err := timeoutContextFromParent(ctx, 10*time.Second, "AnLF backend runtime release")
	if err != nil {
		return err
	}
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		c.subscriptionURL(subscriptionID, "/runtime"),
		nil,
	)
	if err != nil {
		return fmt.Errorf("create AnLF runtime release request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send AnLF runtime release request: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.AnlfLog.Debugf("failed to close response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("AnLF backend runtime release failed: status=%d", resp.StatusCode)
	}

	return nil
}
