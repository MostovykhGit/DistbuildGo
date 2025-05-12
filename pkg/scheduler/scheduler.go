//go:build !solution

package scheduler

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"gitlab.com/slon/shad-go/distbuild/pkg/api"
	"gitlab.com/slon/shad-go/distbuild/pkg/build"
)

var TimeAfter = time.After

type BlockingQueue struct {
	mutex  sync.Mutex
	cond   *sync.Cond
	queue  []*PendingJob
	closed bool
}

func NewQueue() *BlockingQueue {
	var queue BlockingQueue
	queue.cond = sync.NewCond(&queue.mutex)
	queue.queue = make([]*PendingJob, 0)
	queue.closed = false
	return &queue
}

func (q *BlockingQueue) Put(job *PendingJob) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.closed {
		return
	}

	q.queue = append(q.queue, job)
	if len(q.queue) > 0 {
		q.cond.Broadcast()
	}
}

func (q *BlockingQueue) Take() (*PendingJob, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	for len(q.queue) == 0 && !q.closed {
		q.cond.Wait()
	}

	if len(q.queue) == 0 {
		return nil, false
	}

	job := q.queue[0]
	q.queue = q.queue[1:]
	return job, true
}

func (q *BlockingQueue) Close() {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.closed = true
	q.cond.Broadcast()
}

type PendingJob struct {
	Job      *api.JobSpec
	Finished chan struct{}
	Result   *api.JobResult
}

type Config struct {
	CacheTimeout time.Duration
	DepsTimeout  time.Duration
}

type Scheduler struct {
	queue     *BlockingQueue
	logger    *zap.Logger
	config    Config
	Artifacts map[build.ID]api.WorkerID

	resJobs map[build.ID]*PendingJob
	mutex   sync.Mutex

	helping sync.WaitGroup
}

func NewScheduler(l *zap.Logger, config Config) *Scheduler {
	var sched Scheduler
	sched.queue = NewQueue()
	sched.logger = l
	sched.config = config
	sched.Artifacts = make(map[build.ID]api.WorkerID)
	sched.resJobs = make(map[build.ID]*PendingJob)

	return &sched
}

func (c *Scheduler) LocateArtifact(id build.ID) (api.WorkerID, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	worker, ok := c.Artifacts[id]
	return worker, ok
}

func (c *Scheduler) OnJobComplete(workerID api.WorkerID, jobID build.ID, res *api.JobResult) bool {
	c.mutex.Lock()

	isChanClosed := true
	select {
	case _, isChanClosed = <-c.resJobs[jobID].Finished:
	default:
	}

	if isChanClosed {
		close(c.resJobs[jobID].Finished)
	}

	c.resJobs[jobID].Result.ID = res.ID
	c.resJobs[jobID].Result.ExitCode = res.ExitCode
	c.resJobs[jobID].Result.Stdout = res.Stdout
	c.resJobs[jobID].Result.Stderr = res.Stderr
	c.resJobs[jobID].Result.Error = res.Error
	c.mutex.Unlock()

	return true
}

func (c *Scheduler) ScheduleJob(job *api.JobSpec) *PendingJob {
	c.mutex.Lock()
	if done, ok := c.resJobs[job.ID]; ok {
		c.mutex.Unlock()
		return done
	} else {
		c.mutex.Unlock()
	}

	pending := &PendingJob{}
	pending.Job = job
	pending.Finished = make(chan struct{})
	pending.Result = &api.JobResult{}
	c.queue.Put(pending)

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.resJobs[job.ID] = pending

	return pending
}

func (c *Scheduler) PickJob(ctx context.Context, workerID api.WorkerID) *PendingJob {
	waitJob := make(chan *PendingJob)

	c.helping.Add(1)
	go func() {
		defer c.helping.Done()
		job, _ := c.queue.Take()

		select {
		case waitJob <- job:
		case <-ctx.Done():
		}
	}()

	select {
	case job := <-waitJob:
		c.mutex.Lock()
		c.Artifacts[job.Job.ID] = workerID
		c.mutex.Unlock()
		return job
	case <-ctx.Done():
		return nil
	}
}

func (c *Scheduler) Stop() {
	c.queue.Close()
	c.helping.Wait()
}
