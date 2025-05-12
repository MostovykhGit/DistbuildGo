//go:build !solution

package artifact

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"gitlab.com/slon/shad-go/distbuild/pkg/build"
	"gitlab.com/slon/shad-go/distbuild/pkg/tarstream"
)

// Download artifact from remote cache into local cache.
func Download(ctx context.Context, endpoint string, c *Cache, artifactID build.ID) error {
	url := endpoint + "/artifact?id=" + artifactID.String()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		errorMessage := fmt.Sprintf("status code is not OK in handler - %d", httpResp.StatusCode)
		return errors.New(errorMessage)
	}

	dir, commit, abort, err := c.Create(artifactID)
	if err != nil {
		dir2, unlock, err2 := c.Get(artifactID)
		err = err2
		if err != nil {
			fmt.Println("error in artifact client: " + err.Error())
			return err
		}
		dir = dir2
		commit = func() error {
			unlock()
			return nil
		}
		abort = commit
	}

	err = tarstream.Receive(dir, httpResp.Body)
	if err != nil {
		_ = abort()
		return err
	}

	_ = commit()

	return nil
}
