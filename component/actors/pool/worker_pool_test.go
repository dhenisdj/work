package pool

import (
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/enqueue"
	"github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/context"
	"github.com/dhenisdj/scheduler/component/handler"
	"github.com/dhenisdj/scheduler/component/utils/helper"
	"github.com/dhenisdj/scheduler/config"
	"reflect"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

var ctx = context.New("test_sg", true)

func TestWorkerPoolHandlerValidations(t *testing.T) {
	var cases = []struct {
		fn   interface{}
		good bool
	}{
		{func(j *task.Job) error { return nil }, true},
		{func(c *helper.TstCtx, j *task.Job) error { return nil }, true},
		{func(c *helper.TstCtx, j *task.Job) {}, false},
		{func(c *helper.TstCtx, j *task.Job) string { return "" }, false},
		{func(c *helper.TstCtx, j *task.Job) (error, string) { return nil, "" }, false},
		{func(c *helper.TstCtx) error { return nil }, false},
		{func(c helper.TstCtx, j *task.Job) error { return nil }, false},
		{func() error { return nil }, false},
		{func(c *helper.TstCtx, j *task.Job, wat string) error { return nil }, false},
	}

	for i, testCase := range cases {
		r := IsValidHandlerType(helper.TstCtxType, reflect.ValueOf(testCase.fn))
		if testCase.good != r {
			t.Errorf("idx %d: should return %v but returned %v", i, testCase.good, r)
		}
	}
}
func TestWorkerPoolMiddlewareValidations(t *testing.T) {
	var cases = []struct {
		fn   interface{}
		good bool
	}{
		{func(j *task.Job, n handler.NextMiddlewareFunc) error { return nil }, true},
		{func(c *helper.TstCtx, j *task.Job, n handler.NextMiddlewareFunc) error { return nil }, true},
		{func(c *helper.TstCtx, j *task.Job) error { return nil }, false},
		{func(c *helper.TstCtx, j *task.Job, n handler.NextMiddlewareFunc) {}, false},
		{func(c *helper.TstCtx, j *task.Job, n handler.NextMiddlewareFunc) string { return "" }, false},
		{func(c *helper.TstCtx, j *task.Job, n handler.NextMiddlewareFunc) (error, string) { return nil, "" }, false},
		{func(c *helper.TstCtx, n handler.NextMiddlewareFunc) error { return nil }, false},
		{func(c helper.TstCtx, j *task.Job, n handler.NextMiddlewareFunc) error { return nil }, false},
		{func() error { return nil }, false},
		{func(c *helper.TstCtx, j *task.Job, wat string) error { return nil }, false},
		{func(c *helper.TstCtx, j *task.Job, n handler.NextMiddlewareFunc, wat string) error { return nil }, false},
	}

	for i, testCase := range cases {
		r := IsValidMiddlewareType(helper.TstCtxType, reflect.ValueOf(testCase.fn))
		if testCase.good != r {
			t.Errorf("idx %d: should return %v but returned %v", i, testCase.good, r)
		}
	}
}

func TestWorkerPoolStartStop(t *testing.T) {
	wp := NewWorkerPool(context.New("test_sg", true))
	wp.Start()
	wp.Start()
	wp.Stop()
	wp.Stop()
	wp.Start()
	wp.Stop()
}

func TestWorkerPoolValidations(t *testing.T) {
	wp := NewWorkerPool(context.New("test_sg", true))

	func() {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				assert.Regexp(t, "Your middleware function can have one of these signatures", fmt.Sprintf("%v", panicErr))
			} else {
				t.Errorf("expected a panic when using bad middleware")
			}
		}()

		wp.Middleware(TestWorkerPoolValidations)
	}()

	func() {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				assert.Regexp(t, "Your handler function can have one of these signatures", fmt.Sprintf("%v", panicErr))
			} else {
				t.Errorf("expected a panic when using a bad handler")
			}
		}()

		wp.Job("wat", TestWorkerPoolValidations)
	}()
}

func TestWorkersPoolRunSingleThreaded(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	job1 := "job1"
	numJobs, sleepTime := 5, 2
	wp := setupTestWorkerPool(pool, ns, job1, task.JobOptions{Priority: 1, MaxConcurrency: 1})
	wp.Start()
	// enqueue some jobs
	enqueuer := enqueue.NewEnqueuer(ctx, ns, "", "")
	for i := 0; i < numJobs; i++ {
		_, err := enqueuer.Enqueue(job1, task.Q{"sleep": sleepTime})
		assert.Nil(t, err)
	}

	// make sure we've enough jobs queued up to make an interesting test
	jobsQueued := helper.ListSize(pool, models.RedisKey2Job(ns, job1))
	assert.True(t, jobsQueued >= 3, "should be at least 3 jobs queued up, but only found %v", jobsQueued)

	// now make sure the during the duration of task execution there is never > 1 task in flight
	start := time.Now()
	totalRuntime := time.Duration(sleepTime*numJobs) * time.Millisecond
	time.Sleep(10 * time.Millisecond)
	for time.Since(start) < totalRuntime {
		// jobs in progress, lock count for the task and lock info for the pool should never exceed 1
		jobsInProgress := helper.ListSize(pool, models.RedisKey2JobInProgress(ns, wp.PoolID, job1))
		assert.True(t, jobsInProgress <= 1, "jobsInProgress should never exceed 1: actual=%d", jobsInProgress)

		jobLockCount := helper.GetInt64(pool, models.RedisKey2JobLock(ns, job1))
		assert.True(t, jobLockCount <= 1, "global lock count for task should never exceed 1, got: %v", jobLockCount)
		wpLockCount := helper.HgetInt64(pool, models.RedisKey2JobLockInfo(ns, job1), wp.PoolID)
		assert.True(t, wpLockCount <= 1, "lock count for the work pool should never exceed 1: actual=%v", wpLockCount)
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
	wp.Drain()
	wp.Stop()

	// At this point it should all be empty.
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2Job(ns, job1)))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2JobInProgress(ns, wp.PoolID, job1)))
	assert.EqualValues(t, 0, helper.GetInt64(pool, models.RedisKey2JobLock(ns, job1)))
	assert.EqualValues(t, 0, helper.HgetInt64(pool, models.RedisKey2JobLockInfo(ns, job1), wp.PoolID))
}

func TestWorkerPoolPauseSingleThreadedJobs(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns, job1 := "work", "job1"
	numJobs, sleepTime := 5, 2
	wp := setupTestWorkerPool(pool, ns, job1, task.JobOptions{Priority: 1, MaxConcurrency: 1})
	wp.Start()
	// enqueue some jobs
	enqueuer := enqueue.NewEnqueuer(ctx, ns, "", "")
	for i := 0; i < numJobs; i++ {
		_, err := enqueuer.Enqueue(job1, task.Q{"sleep": sleepTime})
		assert.Nil(t, err)
	}
	// provide time for jobs to process
	time.Sleep(10 * time.Millisecond)

	// pause work, provide time for outstanding jobs to finish and queue up another task
	helper.PauseJobs(ns, job1, pool)
	time.Sleep(2 * time.Millisecond)
	_, err := enqueuer.Enqueue(job1, task.Q{"sleep": sleepTime})
	assert.Nil(t, err)

	// check that we still have some jobs to process
	assert.True(t, helper.ListSize(pool, models.RedisKey2Job(ns, job1)) >= 1)

	// now make sure no jobs get started until we unpause
	start := time.Now()
	totalRuntime := time.Duration(sleepTime*numJobs) * time.Millisecond
	for time.Since(start) < totalRuntime {
		assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2JobInProgress(ns, wp.PoolID, job1)))
		// lock count for the task and lock info for the pool should both be at 1 while task is running
		assert.EqualValues(t, 0, helper.GetInt64(pool, models.RedisKey2JobLock(ns, job1)))
		assert.EqualValues(t, 0, helper.HgetInt64(pool, models.RedisKey2JobLockInfo(ns, job1), wp.PoolID))
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}

	// unpause work and get past the backoff time
	helper.UnpauseJobs(ns, job1, pool)
	time.Sleep(10 * time.Millisecond)

	wp.Drain()
	wp.Stop()

	// At this point it should all be empty.
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2Job(ns, job1)))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2JobInProgress(ns, wp.PoolID, job1)))
	assert.EqualValues(t, 0, helper.GetInt64(pool, models.RedisKey2JobLock(ns, job1)))
	assert.EqualValues(t, 0, helper.HgetInt64(pool, models.RedisKey2JobLockInfo(ns, job1), wp.PoolID))
}

func setupTestWorkerPool(pool *redis.Pool, namespace, jobName string, jobOpts task.JobOptions) *WorkerPool {
	helper.DeleteQueue(pool, namespace, jobName)
	helper.DeleteRetryAndDead(pool, namespace)
	helper.DeletePausedAndLockedKeys(namespace, jobName, pool)
	wp := NewWorkerPool(context.New("test_sg", true))
	wp.JobWithOptions(jobName, jobOpts, (*helper.TestContext).SleepyJob)
	// reset the backoff times to help with testing
	config.SleepBackoffsInMilliseconds = []int64{10, 10, 10, 10, 10}
	return wp
}
