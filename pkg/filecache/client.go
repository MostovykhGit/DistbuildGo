//go:build !solution

package filecache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/build"
)

type Client struct {
	logger   *zap.Logger
	endpoint string
}

func NewClient(l *zap.Logger, endpoint string) *Client {
	var client Client
	client.logger = l
	client.endpoint = endpoint
	return &client
}

func (c *Client) Upload(ctx context.Context, id build.ID, localPath string) error {
	url := c.endpoint + "/file?id=" + id.String()

	content, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}
	body := bytes.NewBuffer(content)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPut, url, body)
	if err != nil {
		return err
	}

	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		errorMessage := fmt.Sprintf("status code is not OK in upload handler - %d", httpResp.StatusCode)
		return errors.New(errorMessage)
	}

	return nil
}

func (c *Client) Download(ctx context.Context, localCache *Cache, id build.ID) error {
	url := c.endpoint + "/file?id=" + id.String()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return err
	}

	defer httpResp.Body.Close()
	content, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return err
	}

	writer, abort, err := localCache.Write(id)
	if err != nil {
		return err
	}
	defer writer.Close()

	_, err = writer.Write(content)
	if err != nil {
		_ = abort()
		return err
	}

	return nil
}
