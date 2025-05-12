//go:build !solution

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"go.uber.org/zap"
)

type HeartbeatClient struct {
	logger   *zap.Logger
	endpoint string
}

func NewHeartbeatClient(l *zap.Logger, endpoint string) *HeartbeatClient {
	l.Info("new heartbeat client")
	var client HeartbeatClient
	client.logger = l
	client.endpoint = endpoint + "/heartbeat"
	return &client
}

func (c *HeartbeatClient) Heartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	c.logger.Info("heartbeat by client")
	var jsonEncoding []byte
	jsonEncoding, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	body := bytes.NewBuffer(jsonEncoding)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, body)
	httpReq.Header.Set("Content-Type", "application/json")
	if err != nil {
		return nil, err
	}

	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	var resp HeartbeatResponse
	respText, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}

	if httpResp.StatusCode != http.StatusOK {
		errorMessage := fmt.Sprintf("got not OK status code - %d in client heartbeat - %s", httpResp.StatusCode, respText)
		c.logger.Error(errorMessage)
		return nil, errors.New(errorMessage)
	}

	err = json.Unmarshal(respText, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}
