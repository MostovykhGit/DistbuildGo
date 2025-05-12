//go:build !solution

package worker

import (
	"context"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/api"
	"gitlab.com/slon/shad-go/distbuild/pkg/artifact"
	"gitlab.com/slon/shad-go/distbuild/pkg/build"
	"gitlab.com/slon/shad-go/distbuild/pkg/filecache"
)

type Worker struct {
	workerID        api.WorkerID
	coordEndpoint   string
	logger          *zap.Logger
	fileCache       *filecache.Cache
	artifacts       *artifact.Cache
	heartbeatClient *api.HeartbeatClient
	filecacheClient *filecache.Client

	mux *http.ServeMux
}

func New(
	workerID api.WorkerID,
	coordinatorEndpoint string,
	log *zap.Logger,
	fileCache *filecache.Cache,
	artifacts *artifact.Cache,
) *Worker {
	var worker Worker
	worker.workerID = workerID
	worker.coordEndpoint = coordinatorEndpoint
	worker.logger = log
	worker.fileCache = fileCache
	worker.artifacts = artifacts
	worker.heartbeatClient = api.NewHeartbeatClient(log, coordinatorEndpoint)
	worker.filecacheClient = filecache.NewClient(log, coordinatorEndpoint)
	worker.mux = http.NewServeMux()

	artifactHandler := artifact.NewHandler(log, artifacts)
	artifactHandler.Register(worker.mux)

	return &worker
}

func (w *Worker) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	w.mux.ServeHTTP(rw, r)
}

func EnsureBaseDir(fpath string) error {
	baseDir := path.Dir(fpath)
	info, err := os.Stat(baseDir)
	if err == nil && info.IsDir() {
		return nil
	}
	return os.MkdirAll(baseDir, 0755)
}

func (w *Worker) downloadSourceFiles(ctx context.Context, job *api.JobSpec, destDir string) error {
	for id, newFile := range job.SourceFiles {
		err := w.filecacheClient.Download(ctx, w.fileCache, id)
		if err != nil {
			return err
		}
		path, unlock, innerErr := w.fileCache.Get(id)
		err = innerErr
		if err != nil {
			return err
		}
		w.logger.Sugar().Infof("ok get")

		source, innerErr := os.Open(path)
		err = innerErr
		if err != nil {
			unlock()
			return err
		}
		defer source.Close()

		tmpFilename := filepath.Join(destDir, newFile)
		err = EnsureBaseDir(tmpFilename)
		if err != nil {
			w.logger.Sugar().Infof(err.Error())
			return err
		}
		dest, innerErr := os.Create(tmpFilename)
		err = innerErr
		if err != nil {
			unlock()
			return err
		}
		defer dest.Close()

		_, err = io.Copy(dest, source)
		if err != nil {
			unlock()
			return err
		}

		unlock()
	}
	return nil
}

func (w *Worker) downloadArtifacts(ctx context.Context, job *api.JobSpec) (map[build.ID]string, error) {
	depsCtx := make(map[build.ID]string)
	for id, remoteWorker := range job.Artifacts {
		err := artifact.Download(ctx, remoteWorker.String(), w.artifacts, id)
		if err != nil {
			return nil, err
		}

		artPath, unlock, innerErr := w.artifacts.Get(id)
		err = innerErr
		if err != nil {
			return nil, err
		}
		depsCtx[id] = artPath
		unlock()
	}

	return depsCtx, nil
}

func (w *Worker) Run(ctx context.Context) error {
	finished := make([]api.JobResult, 0)
	addedArtifacts := make([]build.ID, 0)
	for {
		req := &api.HeartbeatRequest{
			WorkerID:       w.workerID,
			FreeSlots:      1,
			FinishedJob:    finished,
			AddedArtifacts: addedArtifacts,
		}

		finished = make([]api.JobResult, 0)
		addedArtifacts = make([]build.ID, 0)

		resp, err := w.heartbeatClient.Heartbeat(ctx, req)
		if err != nil {
			return err
		}

		for _, job := range resp.JobsToRun {
			tmpDir, err := os.MkdirTemp("", "file"+job.ID.String())
			if err != nil {
				return err
			}

			err = w.downloadSourceFiles(ctx, &job, tmpDir)
			if err != nil {
				return err
			}
			
			depsCtx, err := w.downloadArtifacts(ctx, &job)
			if err != nil {
				return err
			}

			outputDir, commit, abort, err := w.artifacts.Create(job.ID)
			if err != nil {
				return err
			}

			res := api.JobResult{}
			for _, initCmd := range job.Cmds {
				jobCtx := build.JobContext{
					SourceDir: tmpDir,
					OutputDir: outputDir,
					Deps:      depsCtx,
				}

				rendered, innerErr := initCmd.Render(jobCtx)
				err = innerErr
				if err != nil {
					_ = abort()
					return err
				}

				isCat := false
				if rendered.Exec == nil {
					rendered.Exec = make([]string, 0)
					rendered.Exec = append(rendered.Exec, "bash", "-c")
					rendered.Exec = append(rendered.Exec, "echo -n "+rendered.CatTemplate+" > "+rendered.CatOutput)
					isCat = true
				}

				cmd := exec.Command(rendered.Exec[0], rendered.Exec[1:]...)
				cmd.Env = append(cmd.Env, initCmd.Environ...)
				cmd.Dir = initCmd.WorkingDirectory

				res.ID = job.ID
				stdoutPipe, innerErr := cmd.StdoutPipe()
				err = innerErr
				if err != nil {
					_ = abort()
					return err
				}

				stderrPipe, innerErr := cmd.StderrPipe()
				err = innerErr
				if err != nil {
					_ = abort()
					return err
				}

				err = cmd.Start()
				if err != nil {
					_ = abort()
					return err
				}

				if !isCat {
					cmdStdout, innErr := io.ReadAll(stdoutPipe)
					err = innErr
					if err != nil {
						_ = abort()
						return err
					}
					res.Stdout = append(res.Stdout, cmdStdout...)
				}

				cmdStderr, innerErr := io.ReadAll(stderrPipe)
				err = innerErr
				if err != nil {
					_ = abort()
					return err
				}
				res.Stderr = append(res.Stderr, cmdStderr...)
			}

			_ = commit()
			finished = append(finished, res)
			addedArtifacts = append(addedArtifacts, job.ID)
		}
	}
}
