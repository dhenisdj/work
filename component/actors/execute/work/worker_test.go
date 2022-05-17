package work

import (
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/enqueue"
	pool2 "github.com/dhenisdj/scheduler/component/actors/pool"
	"github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/common/context"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/helper"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/dhenisdj/scheduler/config"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

var ctx = context.New()

func TestWorkerBasics(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	job1 := "job1"
	job2 := "job2"
	job3 := "job3"

	helper.CleanKeyspace(ns, pool)

	var arg1 float64
	var arg2 float64
	var arg3 float64

	jobTypes := make(map[string]*task.JobType)
	jobTypes[job1] = &task.JobType{
		Name:       job1,
		JobOptions: task.JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *task.Job) error {
			arg1 = job.Args["a"].(float64)
			return nil
		},
	}
	jobTypes[job2] = &task.JobType{
		Name:       job2,
		JobOptions: task.JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *task.Job) error {
			arg2 = job.Args["a"].(float64)
			return nil
		},
	}
	jobTypes[job3] = &task.JobType{
		Name:       job3,
		JobOptions: task.JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *task.Job) error {
			arg3 = job.Args["a"].(float64)
			return nil
		},
	}

	enqueuer := enqueue.NewEnqueuer(ctx, ns, "", "", pool)
	_, err := enqueuer.Enqueue(job1, task.Q{"a": 1})
	assert.Nil(t, err)
	_, err = enqueuer.Enqueue(job2, task.Q{"a": 2})
	assert.Nil(t, err)
	_, err = enqueuer.Enqueue(job3, task.Q{"a": 3})
	assert.Nil(t, err)

	w := NewWorker(
		ns,
		"1",
		"1",
		"1",
		WithSingleSession(),
		WithContext(ctx),
		WithRedis(pool),
	)
	w.UpdateMiddlewareAndJobTypes(nil, jobTypes)
	w.start()
	w.drain()
	w.stop()

	// make sure the jobs ran (side effect of setting these variables to the task arguments)
	assert.EqualValues(t, 1.0, arg1)
	assert.EqualValues(t, 2.0, arg2)
	assert.EqualValues(t, 3.0, arg3)

	// nothing in retries or dead
	assert.EqualValues(t, 0, helper.ZsetSize(pool, models.RedisKey2JobRetry(ns)))
	assert.EqualValues(t, 0, helper.ZsetSize(pool, models.RedisKey2JobDead(ns)))

	// Nothing in the queues or in-progress queues
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2Job(ns, job1)))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2Job(ns, job2)))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2Job(ns, job3)))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2JobInProgress(ns, "1", job1)))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2JobInProgress(ns, "1", job2)))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2JobInProgress(ns, "1", job3)))

	// nothing in the work status
	h := helper.ReadHash(pool, models.RedisKey2Observation(ns, w.WorkerID))
	assert.EqualValues(t, 0, len(h))
}

func TestWorkerInProgress(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	job1 := "job1"
	helper.DeleteQueue(pool, ns, job1)
	helper.DeleteRetryAndDead(pool, ns)
	helper.DeletePausedAndLockedKeys(ns, job1, pool)

	jobTypes := make(map[string]*task.JobType)
	jobTypes[job1] = &task.JobType{
		Name:       job1,
		JobOptions: task.JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *task.Job) error {
			time.Sleep(30 * time.Millisecond)
			return nil
		},
	}

	enqueuer := enqueue.NewEnqueuer(ctx, ns, "", "", pool)
	_, err := enqueuer.Enqueue(job1, task.Q{"a": 1})
	assert.Nil(t, err)

	w := NewWorker(
		ns,
		"1",
		"1",
		"1",
		WithSingleSession(),
		WithContext(ctx),
		WithRedis(pool),
	)
	w.UpdateMiddlewareAndJobTypes(nil, jobTypes)
	w.start()

	// instead of w.forceIter(), we'll wait for 10 milliseconds to let the task start
	// The task will then sleep for 30ms. In that time, we should be able to see something in the in-progress queue.
	time.Sleep(10 * time.Millisecond)
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2Job(ns, job1)))
	assert.EqualValues(t, 1, helper.ListSize(pool, models.RedisKey2JobInProgress(ns, "1", job1)))
	assert.EqualValues(t, 1, helper.GetInt64(pool, models.RedisKey2JobLock(ns, job1)))
	assert.EqualValues(t, 1, helper.HgetInt64(pool, models.RedisKey2JobLockInfo(ns, job1), w.poolID))

	// nothing in the work status
	w.Observer.Drain()
	h := helper.ReadHash(pool, models.RedisKey2Observation(ns, w.WorkerID))
	assert.Equal(t, job1, h["job_name"])
	assert.Equal(t, `{"a":1}`, h["args"])
	// NOTE: we could check for job_id and started_at, but it's a PITA and it's tested in observer_test.

	w.drain()
	w.stop()

	// At this point, it should all be empty.
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2Job(ns, job1)))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2JobInProgress(ns, "1", job1)))

	// nothing in the work status
	h = helper.ReadHash(pool, models.RedisKey2Observation(ns, w.WorkerID))
	assert.EqualValues(t, 0, len(h))
}

func TestWorkerRetry(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	job1 := "job1"
	helper.DeleteQueue(pool, ns, job1)
	helper.DeleteRetryAndDead(pool, ns)
	helper.DeletePausedAndLockedKeys(ns, job1, pool)

	jobTypes := make(map[string]*task.JobType)
	jobTypes[job1] = &task.JobType{
		Name:       job1,
		JobOptions: task.JobOptions{Priority: 1, MaxFails: 3},
		IsGeneric:  true,
		GenericHandler: func(job *task.Job) error {
			return fmt.Errorf("sorry kid")
		},
	}

	enqueuer := enqueue.NewEnqueuer(ctx, ns, "", "", pool)
	_, err := enqueuer.Enqueue(job1, task.Q{"a": 1})
	assert.Nil(t, err)
	w := NewWorker(
		ns,
		"1",
		"1",
		"1",
		WithSingleSession(),
		WithContext(ctx),
		WithRedis(pool),
	)
	w.UpdateMiddlewareAndJobTypes(nil, jobTypes)
	w.start()
	w.drain()
	w.stop()

	// Ensure the right stuff is in our queues:
	assert.EqualValues(t, 1, helper.ZsetSize(pool, models.RedisKey2JobRetry(ns)))
	assert.EqualValues(t, 0, helper.ZsetSize(pool, models.RedisKey2JobDead(ns)))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2Job(ns, job1)))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2JobInProgress(ns, "1", job1)))
	assert.EqualValues(t, 0, helper.GetInt64(pool, models.RedisKey2JobLock(ns, job1)))
	assert.EqualValues(t, 0, helper.HgetInt64(pool, models.RedisKey2JobLockInfo(ns, job1), w.poolID))

	// Get the task on the retry queue
	ts, job := helper.JobOnZset(pool, models.RedisKey2JobRetry(ns))

	assert.True(t, ts > utils.NowEpochSeconds())      // enqueued in the future
	assert.True(t, ts < (utils.NowEpochSeconds()+80)) // but less than a minute from now (first failure)

	assert.Equal(t, job1, job.Name) // basics are preserved
	assert.EqualValues(t, 1, job.Fails)
	assert.Equal(t, "sorry kid", job.LastErr)
	assert.True(t, (utils.NowEpochSeconds()-job.FailedAt) <= 2)
}

// Check if a custom backoff function functions functionally.
func TestWorkerRetryWithCustomBackoff(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	job1 := "job1"
	helper.DeleteQueue(pool, ns, job1)
	helper.DeleteRetryAndDead(pool, ns)
	calledCustom := 0

	custombo := func(job *task.Job) int64 {
		calledCustom++
		return 5 // Always 5 seconds
	}

	jobTypes := make(map[string]*task.JobType)
	jobTypes[job1] = &task.JobType{
		Name:       job1,
		JobOptions: task.JobOptions{Priority: 1, MaxFails: 3, Backoff: custombo},
		IsGeneric:  true,
		GenericHandler: func(job *task.Job) error {
			return fmt.Errorf("sorry kid")
		},
	}

	enqueuer := enqueue.NewEnqueuer(ctx, ns, "", "", pool)
	_, err := enqueuer.Enqueue(job1, task.Q{"a": 1})
	assert.Nil(t, err)
	w := NewWorker(
		ns,
		"1",
		"1",
		"1",
		WithSingleSession(),
		WithContext(ctx),
		WithRedis(pool),
	)
	w.UpdateMiddlewareAndJobTypes(nil, jobTypes)
	w.start()
	w.drain()
	w.stop()

	// Ensure the right stuff is in our queues:
	assert.EqualValues(t, 1, helper.ZsetSize(pool, models.RedisKey2JobRetry(ns)))
	assert.EqualValues(t, 0, helper.ZsetSize(pool, models.RedisKey2JobDead(ns)))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2Job(ns, job1)))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2JobInProgress(ns, "1", job1)))

	// Get the task on the retry queue
	ts, job := helper.JobOnZset(pool, models.RedisKey2JobRetry(ns))

	assert.True(t, ts > utils.NowEpochSeconds())      // enqueued in the future
	assert.True(t, ts < (utils.NowEpochSeconds()+10)) // but less than ten secs in

	assert.Equal(t, job1, job.Name) // basics are preserved
	assert.EqualValues(t, 1, job.Fails)
	assert.Equal(t, "sorry kid", job.LastErr)
	assert.True(t, (utils.NowEpochSeconds()-job.FailedAt) <= 2)
	assert.Equal(t, 1, calledCustom)
}

func TestWorkerDead(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	job1 := "job1"
	job2 := "job2"
	helper.DeleteQueue(pool, ns, job1)
	helper.DeleteQueue(pool, ns, job2)
	helper.DeleteRetryAndDead(pool, ns)
	helper.DeletePausedAndLockedKeys(ns, job1, pool)

	jobTypes := make(map[string]*task.JobType)
	jobTypes[job1] = &task.JobType{
		Name:       job1,
		JobOptions: task.JobOptions{Priority: 1, MaxFails: 0},
		IsGeneric:  true,
		GenericHandler: func(job *task.Job) error {
			return fmt.Errorf("sorry kid1")
		},
	}
	jobTypes[job2] = &task.JobType{
		Name:       job2,
		JobOptions: task.JobOptions{Priority: 1, MaxFails: 0, SkipDead: true},
		IsGeneric:  true,
		GenericHandler: func(job *task.Job) error {
			return fmt.Errorf("sorry kid2")
		},
	}

	enqueuer := enqueue.NewEnqueuer(ctx, ns, "", "", pool)
	_, err := enqueuer.Enqueue(job1, nil)
	assert.Nil(t, err)
	_, err = enqueuer.Enqueue(job2, nil)
	assert.Nil(t, err)
	w := NewWorker(
		ns,
		"1",
		"1",
		"1",
		WithSingleSession(),
		WithContext(ctx),
		WithRedis(pool),
	)
	w.UpdateMiddlewareAndJobTypes(nil, jobTypes)
	w.start()
	w.drain()
	w.stop()

	// Ensure the right stuff is in our queues:
	assert.EqualValues(t, 0, helper.ZsetSize(pool, models.RedisKey2JobRetry(ns)))
	assert.EqualValues(t, 1, helper.ZsetSize(pool, models.RedisKey2JobDead(ns)))

	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2Job(ns, job1)))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2JobInProgress(ns, "1", job1)))
	assert.EqualValues(t, 0, helper.GetInt64(pool, models.RedisKey2JobLock(ns, job1)))
	assert.EqualValues(t, 0, helper.HgetInt64(pool, models.RedisKey2JobLockInfo(ns, job1), w.poolID))

	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2Job(ns, job2)))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2JobInProgress(ns, "1", job2)))
	assert.EqualValues(t, 0, helper.GetInt64(pool, models.RedisKey2JobLock(ns, job2)))
	assert.EqualValues(t, 0, helper.HgetInt64(pool, models.RedisKey2JobLockInfo(ns, job2), w.poolID))

	// Get the task on the dead queue
	ts, job := helper.JobOnZset(pool, models.RedisKey2JobDead(ns))

	assert.True(t, ts <= utils.NowEpochSeconds())

	assert.Equal(t, job1, job.Name) // basics are preserved
	assert.EqualValues(t, 1, job.Fails)
	assert.Equal(t, "sorry kid1", job.LastErr)
	assert.True(t, (utils.NowEpochSeconds()-job.FailedAt) <= 2)
}

func TestWorkersPaused(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	job1 := "job1"
	helper.DeleteQueue(pool, ns, job1)
	helper.DeleteRetryAndDead(pool, ns)
	helper.DeletePausedAndLockedKeys(ns, job1, pool)

	jobTypes := make(map[string]*task.JobType)
	jobTypes[job1] = &task.JobType{
		Name:       job1,
		JobOptions: task.JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *task.Job) error {
			time.Sleep(30 * time.Millisecond)
			return nil
		},
	}

	enqueuer := enqueue.NewEnqueuer(ctx, ns, "", "", pool)
	_, err := enqueuer.Enqueue(job1, task.Q{"a": 1})
	assert.Nil(t, err)

	w := NewWorker(
		ns,
		"1",
		"1",
		"1",
		WithSingleSession(),
		WithContext(ctx),
		WithRedis(pool),
	)
	w.UpdateMiddlewareAndJobTypes(nil, jobTypes)
	// pause the jobs prior to starting
	err = helper.PauseJobs(ns, job1, pool)
	assert.Nil(t, err)
	// reset the backoff times to help with testing
	config.SleepBackoffsInMilliseconds = []int64{10, 10, 10, 10, 10}
	w.start()

	// make sure the jobs stay in the still in the run queue and not moved to in progress
	for i := 0; i < 2; i++ {
		time.Sleep(10 * time.Millisecond)
		assert.EqualValues(t, 1, helper.ListSize(pool, models.RedisKey2Job(ns, job1)))
		assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2JobInProgress(ns, "1", job1)))
	}

	// now unpause the jobs and check that they start
	err = helper.UnpauseJobs(ns, job1, pool)
	assert.Nil(t, err)
	// sleep through 2 backoffs to make sure we allow enough time to start running
	time.Sleep(20 * time.Millisecond)
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2Job(ns, job1)))
	assert.EqualValues(t, 1, helper.ListSize(pool, models.RedisKey2JobInProgress(ns, "1", job1)))

	w.Observer.Drain()
	h := helper.ReadHash(pool, models.RedisKey2Observation(ns, w.WorkerID))
	assert.Equal(t, job1, h["job_name"])
	assert.Equal(t, `{"a":1}`, h["args"])
	w.drain()
	w.stop()

	// At this point, it should all be empty.
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2Job(ns, job1)))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2JobInProgress(ns, "1", job1)))

	// nothing in the work status
	h = helper.ReadHash(pool, models.RedisKey2Observation(ns, w.WorkerID))
	assert.EqualValues(t, 0, len(h))
}

// Test that in the case of an unavailable Redis server,
// the work loop exits in the case of a WorkerPool.Stop
func TestStop(t *testing.T) {
	configuration := config.InitConfig("test")
	redisPool := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "notworking:6379", redis.DialConnectTimeout(1*time.Second))
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}
	wp := pool2.NewWorkerPool(context.New(), configuration, redisPool)
	wp.Start()
	wp.Stop()
}

func BenchmarkJobProcessing(b *testing.B) {
	configuration := config.InitConfig("test")
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)
	enqueuer := enqueue.NewEnqueuer(ctx, ns, "", "", pool)

	for i := 0; i < b.N; i++ {
		_, err := enqueuer.Enqueue("wat", nil)
		if err != nil {
			panic(err)
		}
	}

	wp := pool2.NewWorkerPool(ctx, configuration, pool)
	wp.Job("wat", func(c *helper.TestContext, job *task.Job) error {
		return nil
	})

	b.ResetTimer()

	wp.Start()
	wp.Drain()
	wp.Stop()
}

type emptyCtx struct{}

// Starts up a pool with two workers emptying it as fast as they can
// The pool is Stop()ped while jobs are still going on.  Tests that the
// pool processing is really stopped and that it's not first completely
// drained before returning.
// https://github.com/dhenisdj/scheduler/issues/24
func TestWorkerPoolStop(t *testing.T) {
	configuration := config.InitConfig("test")
	ns := "will_it_end"
	pool := helper.NewTestPool(":6379")
	var started, stopped int32
	num_iters := 30

	wp := pool2.NewWorkerPool(ctx, configuration, pool)

	wp.Job("sample_job", func(c *emptyCtx, job *task.Job) error {
		atomic.AddInt32(&started, 1)
		time.Sleep(1 * time.Second)
		atomic.AddInt32(&stopped, 1)
		return nil
	})

	var enqueuer = enqueue.NewEnqueuer(ctx, ns, "", "", pool)

	for i := 0; i <= num_iters; i++ {
		enqueuer.Enqueue("sample_job", task.Q{})
	}

	// Start the pool and quit before it has had a chance to complete
	// all the jobs.
	wp.Start()
	time.Sleep(5 * time.Second)
	wp.Stop()

	if started != stopped {
		t.Errorf("Expected that jobs were finished and not killed while processing (started=%d, stopped=%d)", started, stopped)
	}

	if started >= int32(num_iters) {
		t.Errorf("Expected that jobs queue was not completely emptied.")
	}
}
