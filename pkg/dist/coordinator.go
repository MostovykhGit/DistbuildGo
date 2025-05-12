//go:build !solution

package dist

import (
	"context"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/api"
	"gitlab.com/slon/shad-go/distbuild/pkg/build"
	"gitlab.com/slon/shad-go/distbuild/pkg/filecache"
	"gitlab.com/slon/shad-go/distbuild/pkg/scheduler"
)

type Coordinator struct {
	logger    *zap.Logger
	fileCache *filecache.Cache
	mux       *http.ServeMux

	sched *scheduler.Scheduler

	waitSignal sync.WaitGroup
	innerMutex sync.Mutex
}

var defaultConfig = scheduler.Config{
	CacheTimeout: time.Millisecond * 10,
	DepsTimeout:  time.Millisecond * 100,
}

func NewCoordinator(
	log *zap.Logger,
	fileCache *filecache.Cache,
) *Coordinator {
	var coord Coordinator
	coord.logger = log
	coord.fileCache = fileCache
	coord.mux = http.NewServeMux()
	coord.sched = scheduler.NewScheduler(log, defaultConfig)

	heartbeatHandler := api.NewHeartbeatHandler(log, &coord)
	buildHandler := api.NewBuildService(log, &coord)
	filecacheHandler := filecache.NewHandler(log, coord.fileCache)

	heartbeatHandler.Register(coord.mux)
	buildHandler.Register(coord.mux)
	filecacheHandler.Register(coord.mux)

	return &coord
}

func (c *Coordinator) Stop() {
	c.sched.Stop()
}

func (c *Coordinator) StartBuild(ctx context.Context, request *api.BuildRequest, w api.StatusWriter) error {
	jobs := build.TopSort(request.Graph.Jobs)
	graph := request.Graph

	missed := make([]build.ID, 0)
	for id := range request.Graph.SourceFiles {
		_, unlock, err := c.fileCache.Get(id)
		if err != nil {
			c.logger.Sugar().Infof("error on file with id %s", id.String())
			missed = append(missed, id)
		} else {
			unlock()
		}
	}

	c.waitSignal.Add(1)
	err := w.Started(&api.BuildStarted{
		ID:           build.NewID(),
		MissingFiles: missed,
	})

	c.waitSignal.Wait()

	for _, job := range jobs {
		var jobSpec api.JobSpec
		jobSpec.Job = job
		jobSpec.SourceFiles = make(map[build.ID]string)
		for id, path := range graph.SourceFiles {
			for _, input := range job.Inputs {
				if input == path {
					jobSpec.SourceFiles[id] = path
					break
				}
			}
		}
		
		jobSpec.Artifacts = make(map[build.ID]api.WorkerID)
		for _, depID := range job.Deps {
			workerID, _ := c.sched.LocateArtifact(depID)
			jobSpec.Artifacts[depID] = workerID
		}

		pending := c.sched.ScheduleJob(&jobSpec)

		select {
		case <-pending.Finished:
		case <-ctx.Done():
			c.logger.Sugar().Infof("context done while job is pending")
			return ctx.Err()
		}

		c.innerMutex.Lock()
		_ = w.Updated(&api.StatusUpdate{
			JobFinished:   pending.Result,
			BuildFinished: &api.BuildFinished{},
		})
		c.innerMutex.Unlock()
	}

	if err != nil {
		return err
	}
	return nil
}

func (c *Coordinator) SignalBuild(ctx context.Context, buildID build.ID, signal *api.SignalRequest) (*api.SignalResponse, error) {
	c.waitSignal.Done()
	return &api.SignalResponse{}, nil
}

func (c *Coordinator) Heartbeat(ctx context.Context, req *api.HeartbeatRequest) (*api.HeartbeatResponse, error) {
	c.innerMutex.Lock()
	for _, finishedJob := range req.FinishedJob {
		c.sched.OnJobComplete(req.WorkerID, finishedJob.ID, &finishedJob)
	}
	c.innerMutex.Unlock()

	var resp *api.HeartbeatResponse
	if req.FreeSlots > 0 {
		job := c.sched.PickJob(ctx, req.WorkerID)

		if job != nil {
			resp = &api.HeartbeatResponse{
				JobsToRun: map[build.ID]api.JobSpec{
					job.Job.ID: *job.Job,
				},
			}
		}
	} else {
		resp = &api.HeartbeatResponse{
			JobsToRun: make(map[build.ID]api.JobSpec),
		}
	}

	return resp, nil
}

func (c *Coordinator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mux.ServeHTTP(w, r)
}
