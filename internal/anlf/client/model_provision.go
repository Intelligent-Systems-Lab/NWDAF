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
	body, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal model provision event: %w", err)
	}
	requestCtx, cancel, err := timeoutContextFromParent(ctx, 120*time.Second, "apply model provision event")
	if err != nil {
		return nil, err
	}
	defer cancel()
	req, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodPost,
		c.endpoint+"/model-provision-events",
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, fmt.Errorf("create model provision event request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send model provision event: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.AnlfLog.Debugf("failed to close model provision event response: %v", closeErr)
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("apply model provision event: status=%d", resp.StatusCode)
	}
	var result contract.ModelProvisionEventResponse
	if decodeErr := json.NewDecoder(resp.Body).Decode(&result); decodeErr != nil {
		return nil, fmt.Errorf("decode model provision event response: %w", decodeErr)
	}
	return &result, nil
}
