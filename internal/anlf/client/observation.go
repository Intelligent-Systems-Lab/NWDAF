package client

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/free5gc/nwdaf/internal/anlf/contract"
)

func (c *Client) SyncObservationBindings(
	ctx context.Context,
	subscriptionID string,
	request contract.SyncObservationBindingsRequest,
) error {
	return c.sendJSON(
		ctx,
		http.MethodPut,
		c.subscriptionURL(subscriptionID, "/observation-bindings"),
		request,
		http.StatusNoContent,
		10*time.Second,
		"sync AnLF observation bindings",
	)
}

func (c *Client) SendObservations(
	ctx context.Context,
	sourceID string,
	batch contract.ObservationBatch,
) error {
	requestURL := c.endpoint + "/observation-sources/" + url.PathEscape(sourceID) + "/observations"
	return c.sendJSON(
		ctx,
		http.MethodPost,
		requestURL,
		batch,
		http.StatusNoContent,
		10*time.Second,
		"send AnLF observations",
	)
}
