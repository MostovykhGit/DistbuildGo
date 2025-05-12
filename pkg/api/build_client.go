//go:build !solution

package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/build"
)

type BuildClient struct {
	logger   *zap.Logger
	endpoint string
}

func NewBuildClient(l *zap.Logger, endpoint string) *BuildClient {
	var client BuildClient
	client.logger = l
	client.endpoint = endpoint
	return &client
}

type BuildStatusReader struct {
	httpResponse *http.Response
	scanner      bufio.Reader
	logger       *zap.Logger
	isFinish     bool
}

func NewBuildStatusReader(httpResp *http.Response, logger *zap.Logger) *BuildStatusReader {
	var statusReader BuildStatusReader
	statusReader.httpResponse = httpResp
	statusReader.scanner = *bufio.NewReader(httpResp.Body)
	statusReader.logger = logger
	return &statusReader
}

func (reader *BuildStatusReader) Next() (*StatusUpdate, error) {
	reader.logger.Info("Next")
	if reader.isFinish {
		return nil, io.EOF
	}

	var err error
	text, err := reader.scanner.ReadBytes('\n')
	reader.logger.Sugar().Infof("text in next %s", string(text))
	if err != nil {
		reader.logger.Sugar().Infof("error Scan in Next - %s", err.Error())
		return nil, err
	}

	// text = text[:len(text) - 1]

	var statusUpdate StatusUpdate
	err = json.Unmarshal(text, &statusUpdate)
	if err != nil {
		reader.logger.Info("error after unmarshal in Next")
		reader.logger.Info(string(text))
		return nil, err
	}
	if statusUpdate.BuildFailed != nil {
		if statusUpdate.BuildFailed.Error == io.EOF.Error() {
			reader.isFinish = true
			return nil, io.EOF
		}
	}

	return &statusUpdate, nil
}

func (reader BuildStatusReader) Close() error {
	reader.logger.Info("Close")
	err := reader.httpResponse.Body.Close()
	return err
}

func (c *BuildClient) StartBuild(ctx context.Context, request *BuildRequest) (*BuildStarted, StatusReader, error) {
	c.logger.Info("build by client")
	var jsonEncoding []byte
	jsonEncoding, err := json.Marshal(request)
	if err != nil {
		c.logger.Error("encoding json failure in client build")
		return nil, nil, err
	}

	body := bytes.NewBuffer(jsonEncoding)
	endpoint := c.endpoint + "/build"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, body)
	httpReq.Header.Set("Content-Type", "application/json")
	if err != nil {
		return nil, nil, err
	}

	httpResponse, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, nil, err
	}

	if httpResponse.StatusCode != http.StatusOK || httpResponse.Header.Get("Content-Type") != "application/json" {
		defer httpResponse.Body.Close()
		fmt.Println(httpResponse.Header.Get("Content-Type"))
		respText, innerErr := io.ReadAll(httpResponse.Body)
		err = innerErr
		if err != nil {
			return nil, nil, err
		}
		return nil, nil, errors.New(string(respText))
	}

	statusReader := NewBuildStatusReader(httpResponse, c.logger)

	buildStartedJSON, err := statusReader.scanner.ReadBytes('\n')
	if err != nil {
		return nil, nil, err
	}
	var buildStarted BuildStarted
	err = json.Unmarshal(buildStartedJSON, &buildStarted)
	if err != nil {
		c.logger.Error(string(buildStartedJSON))
		return nil, nil, err
	}

	return &buildStarted, statusReader, nil
}

func (c *BuildClient) SignalBuild(ctx context.Context, buildID build.ID, signal *SignalRequest) (*SignalResponse, error) {
	var jsonEncoding []byte
	jsonEncoding, err := json.Marshal(signal)
	if err != nil {
		return nil, err
	}

	body := bytes.NewBuffer(jsonEncoding)
	endpoint := c.endpoint + "/signal?build_id=" + buildID.String()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, body)
	httpReq.Header.Set("Content-Type", "application/json")
	if err != nil {
		return nil, err
	}

	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	var resp SignalResponse
	respText, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}

	if httpResp.StatusCode != http.StatusOK {
		errorMessage := fmt.Sprintf("got not OK status code - %d in signal client - %s", httpResp.StatusCode, respText)
		c.logger.Error(errorMessage)
		return nil, errors.New(errorMessage)
	}

	err = json.Unmarshal(respText, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}
