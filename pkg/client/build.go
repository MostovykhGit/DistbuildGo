//go:build !solution

package client

import (
	"context"
	"errors"
	"io"
	"path/filepath"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/api"
	"gitlab.com/slon/shad-go/distbuild/pkg/build"
	"gitlab.com/slon/shad-go/distbuild/pkg/filecache"
)

type Client struct {
	logger          *zap.Logger
	endpoint        string
	buildClient     *api.BuildClient
	filecacheClient *filecache.Client
	sourceDir       string
}

func NewClient(
	l *zap.Logger,
	apiEndpoint string,
	sourceDir string,
) *Client {
	var client Client
	buildClient := api.NewBuildClient(l, apiEndpoint)
	filecacheClient := filecache.NewClient(l, apiEndpoint)

	client.buildClient = buildClient
	client.filecacheClient = filecacheClient
	client.logger = l
	client.endpoint = apiEndpoint
	client.sourceDir = sourceDir
	return &client
}

type BuildListener interface {
	OnJobStdout(jobID build.ID, stdout []byte) error
	OnJobStderr(jobID build.ID, stderr []byte) error

	OnJobFinished(jobID build.ID) error
	OnJobFailed(jobID build.ID, code int, error string) error
}

func (c *Client) Build(ctx context.Context, graph build.Graph, lsn BuildListener) error {
	buildRequest := &api.BuildRequest{
		Graph: graph,
	}
	buildStarted, reader, err := c.buildClient.StartBuild(ctx, buildRequest)
	if err != nil {
		return err
	}

	for _, missedFile := range buildStarted.MissingFiles {
		filename, ok := graph.SourceFiles[missedFile]
		filename = filepath.Join(c.sourceDir, filename)
		if !ok {
			return errors.New("no such file")
		}

		err = c.filecacheClient.Upload(ctx, missedFile, filename)
		if err != nil {
			return err
		}
	}

	req := &api.SignalRequest{}
	req.UploadDone = &api.UploadDone{}
	_, err = c.buildClient.SignalBuild(ctx, buildStarted.ID, req)
	if err != nil {
		return err
	}

	defer reader.Close()

	for {
		update, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if update.JobFinished == nil {
			continue
		}

		if update.BuildFailed != nil {
			err = lsn.OnJobFailed(update.JobFinished.ID, update.JobFinished.ExitCode, update.BuildFailed.Error)
			if err != nil {
				return err
			}
		}

		if update.BuildFinished != nil {
			err = lsn.OnJobFinished(update.JobFinished.ID)
			if err != nil {
				return err
			}
		}

		if update.JobFinished.Stdout != nil {
			err = lsn.OnJobStdout(update.JobFinished.ID, update.JobFinished.Stdout)
			if err != nil {
				return err
			}
		}

		if update.JobFinished.Stderr != nil {
			err = lsn.OnJobStderr(update.JobFinished.ID, update.JobFinished.Stderr)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
