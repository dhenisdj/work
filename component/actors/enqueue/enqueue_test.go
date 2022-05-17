package enqueue

import (
	"fmt"
	pool2 "github.com/dhenisdj/scheduler/component/actors/pool"
	job2 "github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/helper"
	"github.com/dhenisdj/scheduler/config"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEnqueue(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ctx, ns, "", "", pool)
	job, err := enqueuer.Enqueue("wat", job2.Q{"a": 1, "b": "cool"})
	assert.Nil(t, err)
	assert.Equal(t, "wat", job.Name)
	assert.True(t, len(job.ID) > 10)                        // Something is in it
	assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
	assert.Equal(t, "cool", job.ArgString("b"))
	assert.EqualValues(t, 1, job.ArgInt64("a"))
	assert.NoError(t, job.ArgError())

	// Make sure "wat" is in the known jobs
	assert.EqualValues(t, []string{"wat"}, helper.KnownJobs(pool, models.RedisKey2ValidJobs(ns)))

	// Make sure the cache is set
	expiresAt := enqueuer.knownJobs["wat"]
	assert.True(t, expiresAt > (time.Now().Unix()+290))

	// Make sure the length of the queue is 1
	assert.EqualValues(t, 1, helper.ListSize(pool, models.RedisKey2Job(ns, "wat")))

	// Get the task
	j := helper.JobOnQueue(pool, models.RedisKey2Job(ns, "wat"))
	assert.Equal(t, "wat", j.Name)
	assert.True(t, len(j.ID) > 10)                        // Something is in it
	assert.True(t, j.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, j.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
	assert.Equal(t, "cool", j.ArgString("b"))
	assert.EqualValues(t, 1, j.ArgInt64("a"))
	assert.NoError(t, j.ArgError())

	// Now enqueue another task, make sure that we can enqueue multiple
	_, err = enqueuer.Enqueue("wat", job2.Q{"a": 1, "b": "cool"})
	_, err = enqueuer.Enqueue("wat", job2.Q{"a": 1, "b": "cool"})
	assert.Nil(t, err)
	assert.EqualValues(t, 2, helper.ListSize(pool, models.RedisKey2Job(ns, "wat")))
}

func TestEnqueueIn(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ctx, ns, "", "", pool)

	// Set to expired value to make sure we update the set of known jobs
	enqueuer.knownJobs["wat"] = 4

	job, err := enqueuer.EnqueueIn("wat", 300, job2.Q{"a": 1, "b": "cool"})
	assert.Nil(t, err)
	if assert.NotNil(t, job) {
		assert.Equal(t, "wat", job.Name)
		assert.True(t, len(job.ID) > 10)                        // Something is in it
		assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
		assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
		assert.Equal(t, "cool", job.ArgString("b"))
		assert.EqualValues(t, 1, job.ArgInt64("a"))
		assert.NoError(t, job.ArgError())
		assert.EqualValues(t, job.EnqueuedAt+300, job.RunAt)
	}

	// Make sure "wat" is in the known jobs
	assert.EqualValues(t, []string{"wat"}, helper.KnownJobs(pool, models.RedisKey2ValidJobs(ns)))

	// Make sure the cache is set
	expiresAt := enqueuer.knownJobs["wat"]
	assert.True(t, expiresAt > (time.Now().Unix()+290))

	// Make sure the length of the scheduled task queue is 1
	assert.EqualValues(t, 1, helper.ZsetSize(pool, models.RedisKey2JobScheduled(ns)))

	// Get the task
	score, j := helper.JobOnZset(pool, models.RedisKey2JobScheduled(ns))

	assert.True(t, score > time.Now().Unix()+290)
	assert.True(t, score <= time.Now().Unix()+300)

	assert.Equal(t, "wat", j.Name)
	assert.True(t, len(j.ID) > 10)                        // Something is in it
	assert.True(t, j.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, j.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
	assert.Equal(t, "cool", j.ArgString("b"))
	assert.EqualValues(t, 1, j.ArgInt64("a"))
	assert.NoError(t, j.ArgError())
}

func TestEnqueueUnique(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ctx, ns, "", "", pool)
	var mutex = &sync.Mutex{}
	job, err := enqueuer.EnqueueUnique("wat", job2.Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	if assert.NotNil(t, job) {
		assert.Equal(t, "wat", job.Name)
		assert.True(t, len(job.ID) > 10)                        // Something is in it
		assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
		assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
		assert.Equal(t, "cool", job.ArgString("b"))
		assert.EqualValues(t, 1, job.ArgInt64("a"))
		assert.NoError(t, job.ArgError())
	}

	job, err = enqueuer.EnqueueUnique("wat", job2.Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	assert.Nil(t, job)

	job, err = enqueuer.EnqueueUnique("wat", job2.Q{"a": 1, "b": "coolio"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUnique("wat", nil)
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUnique("wat", nil)
	assert.NoError(t, err)
	assert.Nil(t, job)

	job, err = enqueuer.EnqueueUnique("taw", nil)
	assert.NoError(t, err)
	assert.NotNil(t, job)

	// Process the queues. Ensure the right number of jobs were processed
	var wats, taws int64
	configuration := config.InitConfig("test")
	wp := pool2.NewWorkerPool(ctx, "test", *configuration.Spark.Executor, pool)
	wp.JobWithOptions("wat", job2.JobOptions{Priority: 1, MaxFails: 1}, func(job *job2.Job) error {
		mutex.Lock()
		wats++
		mutex.Unlock()
		return nil
	})
	wp.JobWithOptions("taw", job2.JobOptions{Priority: 1, MaxFails: 1}, func(job *job2.Job) error {
		mutex.Lock()
		taws++
		mutex.Unlock()
		return fmt.Errorf("ohno")
	})
	wp.Start()
	wp.Drain()
	wp.Stop()

	assert.EqualValues(t, 3, wats)
	assert.EqualValues(t, 1, taws)

	// Enqueue again. Ensure we can.
	job, err = enqueuer.EnqueueUnique("wat", job2.Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUnique("wat", job2.Q{"a": 1, "b": "coolio"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	// Even though taw resulted in an error, we should still be able to re-queue it.
	// This could result in multiple taws enqueued at the same time in a production system.
	job, err = enqueuer.EnqueueUnique("taw", nil)
	assert.NoError(t, err)
	assert.NotNil(t, job)
}

func TestEnqueueUniqueIn(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ctx, ns, "", "", pool)

	// Enqueue two unique jobs -- ensure one task sticks.
	job, err := enqueuer.EnqueueUniqueIn("wat", 300, job2.Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	if assert.NotNil(t, job) {
		assert.Equal(t, "wat", job.Name)
		assert.True(t, len(job.ID) > 10)                        // Something is in it
		assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
		assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
		assert.Equal(t, "cool", job.ArgString("b"))
		assert.EqualValues(t, 1, job.ArgInt64("a"))
		assert.NoError(t, job.ArgError())
		assert.EqualValues(t, job.EnqueuedAt+300, job.RunAt)
	}

	job, err = enqueuer.EnqueueUniqueIn("wat", 10, job2.Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	assert.Nil(t, job)

	// Get the task
	score, j := helper.JobOnZset(pool, models.RedisKey2JobScheduled(ns))

	assert.True(t, score > time.Now().Unix()+290) // We don't want to overwrite the time
	assert.True(t, score <= time.Now().Unix()+300)

	assert.Equal(t, "wat", j.Name)
	assert.True(t, len(j.ID) > 10)                        // Something is in it
	assert.True(t, j.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, j.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
	assert.Equal(t, "cool", j.ArgString("b"))
	assert.EqualValues(t, 1, j.ArgInt64("a"))
	assert.NoError(t, j.ArgError())
	assert.True(t, j.Unique)

	// Now try to enqueue more stuff and ensure it
	job, err = enqueuer.EnqueueUniqueIn("wat", 300, job2.Q{"a": 1, "b": "coolio"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUniqueIn("wat", 300, nil)
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUniqueIn("wat", 300, nil)
	assert.NoError(t, err)
	assert.Nil(t, job)

	job, err = enqueuer.EnqueueUniqueIn("taw", 300, nil)
	assert.NoError(t, err)
	assert.NotNil(t, job)
}

func TestEnqueueUniqueByKey(t *testing.T) {
	var arg3 string
	var arg4 string

	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ctx, ns, "", "", pool)
	var mutex = &sync.Mutex{}
	job, err := enqueuer.EnqueueUniqueByKey("wat", job2.Q{"a": 3, "b": "foo"}, job2.Q{"key": "123"})
	assert.NoError(t, err)
	if assert.NotNil(t, job) {
		assert.Equal(t, "wat", job.Name)
		assert.True(t, len(job.ID) > 10)                        // Something is in it
		assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
		assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
		assert.Equal(t, "foo", job.ArgString("b"))
		assert.EqualValues(t, 3, job.ArgInt64("a"))
		assert.NoError(t, job.ArgError())
	}

	job, err = enqueuer.EnqueueUniqueByKey("wat", job2.Q{"a": 3, "b": "bar"}, job2.Q{"key": "123"})
	assert.NoError(t, err)
	assert.Nil(t, job)

	job, err = enqueuer.EnqueueUniqueByKey("wat", job2.Q{"a": 4, "b": "baz"}, job2.Q{"key": "124"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUniqueByKey("taw", nil, job2.Q{"key": "125"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	// Process the queues. Ensure the right number of jobs were processed
	var wats, taws int64
	configuration := config.InitConfig("test")
	wp := pool2.NewWorkerPool(ctx, "test", *configuration.Spark.Executor, pool)
	wp.JobWithOptions("wat", job2.JobOptions{Priority: 1, MaxFails: 1}, func(job *job2.Job) error {
		mutex.Lock()
		argA := job.Args["a"].(float64)
		argB := job.Args["b"].(string)
		if argA == 3 {
			arg3 = argB
		}
		if argA == 4 {
			arg4 = argB
		}

		wats++
		mutex.Unlock()
		return nil
	})
	wp.JobWithOptions("taw", job2.JobOptions{Priority: 1, MaxFails: 1}, func(job *job2.Job) error {
		mutex.Lock()
		taws++
		mutex.Unlock()
		return fmt.Errorf("ohno")
	})
	wp.Start()
	wp.Drain()
	wp.Stop()

	assert.EqualValues(t, 2, wats)
	assert.EqualValues(t, 1, taws)

	// Check that arguments got updated to new value
	assert.EqualValues(t, "bar", arg3)
	assert.EqualValues(t, "baz", arg4)

	// Enqueue again. Ensure we can.
	job, err = enqueuer.EnqueueUniqueByKey("wat", job2.Q{"a": 1, "b": "cool"}, job2.Q{"key": "123"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUniqueByKey("wat", job2.Q{"a": 1, "b": "coolio"}, job2.Q{"key": "124"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	// Even though taw resulted in an error, we should still be able to re-queue it.
	// This could result in multiple taws enqueued at the same time in a production system.
	job, err = enqueuer.EnqueueUniqueByKey("taw", nil, job2.Q{"key": "123"})
	assert.NoError(t, err)
	assert.NotNil(t, job)
}

func EnqueueUniqueInByKey(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ctx, ns, "", "", pool)

	// Enqueue two unique jobs -- ensure one task sticks.
	job, err := enqueuer.EnqueueUniqueInByKey("wat", 300, job2.Q{"a": 1, "b": "cool"}, job2.Q{"key": "123"})
	assert.NoError(t, err)
	if assert.NotNil(t, job) {
		assert.Equal(t, "wat", job.Name)
		assert.True(t, len(job.ID) > 10)                        // Something is in it
		assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
		assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
		assert.Equal(t, "cool", job.ArgString("b"))
		assert.EqualValues(t, 1, job.ArgInt64("a"))
		assert.NoError(t, job.ArgError())
		assert.EqualValues(t, job.EnqueuedAt+300, job.RunAt)
	}

	job, err = enqueuer.EnqueueUniqueInByKey("wat", 10, job2.Q{"a": 1, "b": "cool"}, job2.Q{"key": "123"})
	assert.NoError(t, err)
	assert.Nil(t, job)

	// Get the task
	score, j := helper.JobOnZset(pool, models.RedisKey2JobScheduled(ns))

	assert.True(t, score > time.Now().Unix()+290) // We don't want to overwrite the time
	assert.True(t, score <= time.Now().Unix()+300)

	assert.Equal(t, "wat", j.Name)
	assert.True(t, len(j.ID) > 10)                        // Something is in it
	assert.True(t, j.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, j.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
	assert.Equal(t, "cool", j.ArgString("b"))
	assert.EqualValues(t, 1, j.ArgInt64("a"))
	assert.NoError(t, j.ArgError())
	assert.True(t, j.Unique)
}
