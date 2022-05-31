package client

import (
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/enqueue"
	pool2 "github.com/dhenisdj/scheduler/component/actors/pool"
	"github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/context"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/dhenisdj/scheduler/component/utils/helper"
	"github.com/dhenisdj/scheduler/config"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

var ctx = context.New("test_sg", true)

func TestClientWorkerPoolHeartbeats(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	wp := pool2.NewWorkerPool(ctx)
	wp.Job("wat", func(job *task.Job) error { return nil })
	wp.Job("bob", func(job *task.Job) error { return nil })
	wp.Start()

	wp2 := pool2.NewWorkerPool(ctx)
	wp2.Job("foo", func(job *task.Job) error { return nil })
	wp2.Job("bar", func(job *task.Job) error { return nil })
	wp2.Start()

	time.Sleep(20 * time.Millisecond)

	client := NewClient(ctx, ns)

	hbs, err := client.WorkerPoolHeartbeats()
	assert.NoError(t, err)
	assert.EqualValues(t, 2, len(hbs))
	if len(hbs) == 2 {
		var hbwp, hbwp2 *WorkerPoolHeartbeat

		if wp.PoolID == hbs[0].WorkerPoolID {
			hbwp = hbs[0]
			hbwp2 = hbs[1]
		} else {
			hbwp = hbs[1]
			hbwp2 = hbs[0]
		}

		assert.Equal(t, wp.PoolID, hbwp.WorkerPoolID)
		assert.EqualValues(t, uint(10), hbwp.Concurrency)
		assert.Equal(t, []string{"bob", "wat"}, hbwp.JobNames)
		assert.Equal(t, wp.WorkerIDs(), hbwp.WorkerIDs)

		assert.Equal(t, wp2.PoolID, hbwp2.WorkerPoolID)
		assert.EqualValues(t, uint(11), hbwp2.Concurrency)
		assert.Equal(t, []string{"bar", "foo"}, hbwp2.JobNames)
		assert.Equal(t, wp2.WorkerIDs(), hbwp2.WorkerIDs)
	}

	wp.Stop()
	wp2.Stop()

	hbs, err = client.WorkerPoolHeartbeats()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(hbs))
}

func TestClientWorkerObservations(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	enqueuer := enqueue.NewEnqueuer(ctx, ns, "", "")
	_, err := enqueuer.Enqueue("wat", task.Q{"a": 1, "b": 2})
	assert.Nil(t, err)
	_, err = enqueuer.Enqueue("foo", task.Q{"a": 3, "b": 4})
	assert.Nil(t, err)

	wp := pool2.NewWorkerPool(ctx)
	wp.Job("wat", func(job *task.Job) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	wp.Job("foo", func(job *task.Job) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	wp.Start()

	time.Sleep(10 * time.Millisecond)

	client := NewClient(ctx, ns)
	observations, err := client.WorkerObservations()
	assert.NoError(t, err)
	assert.Equal(t, 10, len(observations))

	watCount := 0
	fooCount := 0
	for _, ob := range observations {
		if ob.JobName == "foo" {
			fooCount++
			assert.True(t, ob.IsBusy)
			assert.Equal(t, `{"a":3,"b":4}`, ob.ArgsJSON)
			assert.True(t, (utils.NowEpochSeconds()-ob.StartedAt) <= 3)
			assert.True(t, ob.JobID != "")
		} else if ob.JobName == "wat" {
			watCount++
			assert.True(t, ob.IsBusy)
			assert.Equal(t, `{"a":1,"b":2}`, ob.ArgsJSON)
			assert.True(t, (utils.NowEpochSeconds()-ob.StartedAt) <= 3)
			assert.True(t, ob.JobID != "")
		} else {
			assert.False(t, ob.IsBusy)
		}
		assert.True(t, ob.WorkerID != "")
	}
	assert.Equal(t, 1, watCount)
	assert.Equal(t, 1, fooCount)

	// time.Sleep(2000 * time.Millisecond)
	//
	// observations, err = client.WorkerObservations()
	// assert.NoError(t, err)
	// assert.Equal(t, 10, len(observations))
	// for _, ob := range observations {
	// 	assert.False(t, ob.IsBusy)
	// 	assert.True(t, ob.WorkerID != "")
	// }

	wp.Stop()

	observations, err = client.WorkerObservations()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(observations))
}

func TestClientQueues(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	enqueuer := enqueue.NewEnqueuer(ctx, ns, "", "")
	_, err := enqueuer.Enqueue("wat", nil)
	_, err = enqueuer.Enqueue("foo", nil)
	_, err = enqueuer.Enqueue("zaz", nil)

	// Start a pool to work on it. It's going to work on the queues
	// side effect of that is knowing which jobs are avail
	wp := pool2.NewWorkerPool(ctx)
	wp.Job("wat", func(job *task.Job) error {
		return nil
	})
	wp.Job("foo", func(job *task.Job) error {
		return nil
	})
	wp.Job("zaz", func(job *task.Job) error {
		return nil
	})
	wp.Start()
	time.Sleep(20 * time.Millisecond)
	wp.Stop()

	utils.SetNowEpochSecondsMock(1425263409)
	defer utils.ResetNowEpochSecondsMock()
	enqueuer.Enqueue("foo", nil)
	utils.SetNowEpochSecondsMock(1425263509)
	enqueuer.Enqueue("foo", nil)
	utils.SetNowEpochSecondsMock(1425263609)
	enqueuer.Enqueue("wat", nil)

	utils.SetNowEpochSecondsMock(1425263709)
	client := NewClient(ctx, ns)
	queues, err := client.Queues()
	assert.NoError(t, err)

	assert.Equal(t, 3, len(queues))
	assert.Equal(t, "foo", queues[0].JobName)
	assert.EqualValues(t, 2, queues[0].Count)
	assert.EqualValues(t, 300, queues[0].Latency)
	assert.Equal(t, "wat", queues[1].JobName)
	assert.EqualValues(t, 1, queues[1].Count)
	assert.EqualValues(t, 100, queues[1].Latency)
	assert.Equal(t, "zaz", queues[2].JobName)
	assert.EqualValues(t, 0, queues[2].Count)
	assert.EqualValues(t, 0, queues[2].Latency)
}

func TestClientScheduledJobs(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	enqueuer := enqueue.NewEnqueuer(ctx, ns, "", "")

	utils.SetNowEpochSecondsMock(1425263409)
	defer utils.ResetNowEpochSecondsMock()
	_, err := enqueuer.EnqueueIn("wat", 0, task.Q{"a": 1, "b": 2})
	_, err = enqueuer.EnqueueIn("zaz", 4, task.Q{"a": 3, "b": 4})
	_, err = enqueuer.EnqueueIn("foo", 2, task.Q{"a": 3, "b": 4})

	client := NewClient(ctx, ns)
	jobs, count, err := client.ScheduledJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(jobs))
	assert.EqualValues(t, 3, count)
	if len(jobs) == 3 {
		assert.EqualValues(t, 1425263409, jobs[0].RunAt)
		assert.EqualValues(t, 1425263411, jobs[1].RunAt)
		assert.EqualValues(t, 1425263413, jobs[2].RunAt)

		assert.Equal(t, "wat", jobs[0].Name)
		assert.Equal(t, "foo", jobs[1].Name)
		assert.Equal(t, "zaz", jobs[2].Name)

		assert.EqualValues(t, 1425263409, jobs[0].EnqueuedAt)
		assert.EqualValues(t, 1425263409, jobs[1].EnqueuedAt)
		assert.EqualValues(t, 1425263409, jobs[2].EnqueuedAt)

		assert.EqualValues(t, interface{}(1), jobs[0].Args["a"])
		assert.EqualValues(t, interface{}(2), jobs[0].Args["b"])

		assert.EqualValues(t, 0, jobs[0].Fails)
		assert.EqualValues(t, 0, jobs[1].Fails)
		assert.EqualValues(t, 0, jobs[2].Fails)

		assert.EqualValues(t, 0, jobs[0].FailedAt)
		assert.EqualValues(t, 0, jobs[1].FailedAt)
		assert.EqualValues(t, 0, jobs[2].FailedAt)

		assert.Equal(t, "", jobs[0].LastErr)
		assert.Equal(t, "", jobs[1].LastErr)
		assert.Equal(t, "", jobs[2].LastErr)
	}
}

func TestClientRetryJobs(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	utils.SetNowEpochSecondsMock(1425263409)
	defer utils.ResetNowEpochSecondsMock()

	enqueuer := enqueue.NewEnqueuer(ctx, ns, "", "")
	_, err := enqueuer.Enqueue("wat", task.Q{"a": 1, "b": 2})
	assert.Nil(t, err)

	utils.SetNowEpochSecondsMock(1425263429)

	wp := pool2.NewWorkerPool(ctx)
	wp.Job("wat", func(job *task.Job) error {
		return fmt.Errorf("ohno")
	})
	wp.Start()
	wp.Drain()
	wp.Stop()

	client := NewClient(ctx, ns)
	jobs, count, err := client.RetryJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))
	assert.EqualValues(t, 1, count)

	if len(jobs) == 1 {
		assert.EqualValues(t, 1425263429, jobs[0].FailedAt)
		assert.Equal(t, "wat", jobs[0].Name)
		assert.EqualValues(t, 1425263409, jobs[0].EnqueuedAt)
		assert.EqualValues(t, interface{}(1), jobs[0].Args["a"])
		assert.EqualValues(t, 1, jobs[0].Fails)
		assert.EqualValues(t, 1425263429, jobs[0].Job.FailedAt)
		assert.Equal(t, "ohno", jobs[0].LastErr)
	}
}

func TestClientDeadJobs(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	utils.SetNowEpochSecondsMock(1425263409)
	defer utils.ResetNowEpochSecondsMock()

	enqueuer := enqueue.NewEnqueuer(ctx, ns, "", "")
	_, err := enqueuer.Enqueue("wat", task.Q{"a": 1, "b": 2})
	assert.Nil(t, err)

	utils.SetNowEpochSecondsMock(1425263429)

	wp := pool2.NewWorkerPool(ctx)
	wp.JobWithOptions("wat", task.JobOptions{Priority: 1, MaxFails: 1}, func(job *task.Job) error {
		return fmt.Errorf("ohno")
	})
	wp.Start()
	wp.Drain()
	wp.Stop()

	client := NewClient(ctx, ns)
	jobs, count, err := client.DeadJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))
	assert.EqualValues(t, 1, count)

	deadJob := jobs[0]
	if len(jobs) == 1 {
		assert.EqualValues(t, 1425263429, jobs[0].FailedAt)
		assert.Equal(t, "wat", jobs[0].Name)
		assert.EqualValues(t, 1425263409, jobs[0].EnqueuedAt)
		assert.EqualValues(t, interface{}(1), jobs[0].Args["a"])
		assert.EqualValues(t, 1, jobs[0].Fails)
		assert.EqualValues(t, 1425263429, jobs[0].Job.FailedAt)
		assert.Equal(t, "ohno", jobs[0].LastErr)
	}

	// Test pagination a bit
	jobs, count, err = client.DeadJobs(2)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))
	assert.EqualValues(t, 1, count)

	// Delete it!
	err = client.DeleteDeadJob(deadJob.DiedAt, deadJob.ID)
	assert.NoError(t, err)

	jobs, count, err = client.DeadJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))
	assert.EqualValues(t, 0, count)
}

func TestClientDeleteDeadJob(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	// Insert a dead task:
	insertDeadJob(ns, pool, "wat", 12345, 12347)
	insertDeadJob(ns, pool, "wat", 12345, 12347)
	insertDeadJob(ns, pool, "wat", 12345, 12349)
	insertDeadJob(ns, pool, "wat", 12345, 12350)

	client := NewClient(ctx, ns)
	jobs, count, err := client.DeadJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(jobs))
	assert.EqualValues(t, 4, count)

	tot := count
	for _, j := range jobs {
		err = client.DeleteDeadJob(j.DiedAt, j.ID)
		assert.NoError(t, err)
		_, count, err = client.DeadJobs(1)
		assert.NoError(t, err)
		assert.Equal(t, tot-1, count)
		tot--
	}

}

func TestClientRetryDeadJob(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	// Insert a dead task:
	insertDeadJob(ns, pool, "wat1", 12345, 12347)
	insertDeadJob(ns, pool, "wat2", 12345, 12347)
	insertDeadJob(ns, pool, "wat3", 12345, 12349)
	insertDeadJob(ns, pool, "wat4", 12345, 12350)

	client := NewClient(ctx, ns)
	jobs, count, err := client.DeadJobs(1)
	assert.NoError(t, err)
	assert.EqualValues(t, 4, len(jobs))
	assert.EqualValues(t, 4, count)

	tot := count
	for _, j := range jobs {
		err = client.RetryDeadJob(j.DiedAt, j.ID)
		assert.NoError(t, err)
		_, count, err = client.DeadJobs(1)
		assert.NoError(t, err)
		assert.Equal(t, tot-1, count)
		tot--
	}

	job1 := getQueuedJob(ns, pool, "wat1")
	assert.NotNil(t, job1)
	assert.Equal(t, "wat1", job1.Name)
	assert.EqualValues(t, 0, job1.Fails)
	assert.Equal(t, "", job1.LastErr)
	assert.EqualValues(t, 0, job1.FailedAt)

	job1 = getQueuedJob(ns, pool, "wat2")
	assert.NotNil(t, job1)
	assert.Equal(t, "wat2", job1.Name)
	assert.EqualValues(t, 0, job1.Fails)
	assert.Equal(t, "", job1.LastErr)
	assert.EqualValues(t, 0, job1.FailedAt)

	job1 = getQueuedJob(ns, pool, "wat3")
	assert.NotNil(t, job1)
	assert.Equal(t, "wat3", job1.Name)
	assert.EqualValues(t, 0, job1.Fails)
	assert.Equal(t, "", job1.LastErr)
	assert.EqualValues(t, 0, job1.FailedAt)

	job1 = getQueuedJob(ns, pool, "wat4")
	assert.NotNil(t, job1)
	assert.Equal(t, "wat4", job1.Name)
	assert.EqualValues(t, 0, job1.Fails)
	assert.Equal(t, "", job1.LastErr)
	assert.EqualValues(t, 0, job1.FailedAt)
}

func TestClientRetryDeadJobWithArgs(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	// Enqueue a task with arguments
	name := "foobar"
	encAt := int64(12345)
	failAt := int64(12347)
	job := &task.Job{
		Name:       name,
		ID:         utils.MakeIdentifier(),
		EnqueuedAt: encAt,
		Args:       map[string]interface{}{"a": "wat"},
		Fails:      3,
		LastErr:    "sorry",
		FailedAt:   failAt,
	}

	rawJSON, _ := job.Serialize()

	conn := pool.Get()
	defer conn.Close()
	_, err := conn.Do("ZADD", models.RedisKey2JobDead(ns), failAt, rawJSON)
	if err != nil {
		panic(err.Error())
	}

	if _, err := conn.Do("SADD", models.RedisKey2ValidJobs(ns), name); err != nil {
		panic(err)
	}

	client := NewClient(ctx, ns)
	err = client.RetryDeadJob(failAt, job.ID)
	assert.NoError(t, err)

	job1 := getQueuedJob(ns, pool, name)
	if assert.NotNil(t, job1) {
		assert.Equal(t, name, job1.Name)
		assert.Equal(t, "wat", job1.ArgString("a"))
		assert.NoError(t, job1.ArgError())
	}
}

func TestClientDeleteAllDeadJobs(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	// Insert a dead task:
	insertDeadJob(ns, pool, "wat", 12345, 12347)
	insertDeadJob(ns, pool, "wat", 12345, 12347)
	insertDeadJob(ns, pool, "wat", 12345, 12349)
	insertDeadJob(ns, pool, "wat", 12345, 12350)

	client := NewClient(ctx, ns)
	jobs, count, err := client.DeadJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(jobs))
	assert.EqualValues(t, 4, count)

	err = client.DeleteAllDeadJobs()
	assert.NoError(t, err)

	jobs, count, err = client.DeadJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))
	assert.EqualValues(t, 0, count)
}

func TestClientRetryAllDeadJobs(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	utils.SetNowEpochSecondsMock(1425263409)
	defer utils.ResetNowEpochSecondsMock()

	insertDeadJob(ns, pool, "wat1", 12345, 12347)
	insertDeadJob(ns, pool, "wat2", 12345, 12347)
	insertDeadJob(ns, pool, "wat3", 12345, 12349)
	insertDeadJob(ns, pool, "wat4", 12345, 12350)

	client := NewClient(ctx, ns)
	jobs, count, err := client.DeadJobs(1)
	assert.NoError(t, err)
	assert.EqualValues(t, 4, len(jobs))
	assert.EqualValues(t, 4, count)

	err = client.RetryAllDeadJobs()
	assert.NoError(t, err)
	_, count, err = client.DeadJobs(1)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, count)

	job := getQueuedJob(ns, pool, "wat1")
	assert.NotNil(t, job)
	assert.Equal(t, "wat1", job.Name)
	assert.EqualValues(t, 1425263409, job.EnqueuedAt)
	assert.EqualValues(t, 0, job.Fails)
	assert.Equal(t, "", job.LastErr)
	assert.EqualValues(t, 0, job.FailedAt)

	job = getQueuedJob(ns, pool, "wat2")
	assert.NotNil(t, job)
	assert.Equal(t, "wat2", job.Name)
	assert.EqualValues(t, 1425263409, job.EnqueuedAt)
	assert.EqualValues(t, 0, job.Fails)
	assert.Equal(t, "", job.LastErr)
	assert.EqualValues(t, 0, job.FailedAt)

	job = getQueuedJob(ns, pool, "wat3")
	assert.NotNil(t, job)
	assert.Equal(t, "wat3", job.Name)
	assert.EqualValues(t, 1425263409, job.EnqueuedAt)
	assert.EqualValues(t, 0, job.Fails)
	assert.Equal(t, "", job.LastErr)
	assert.EqualValues(t, 0, job.FailedAt)

	job = getQueuedJob(ns, pool, "wat4")
	assert.NotNil(t, job)
	assert.Equal(t, "wat4", job.Name)
	assert.EqualValues(t, 1425263409, job.EnqueuedAt)
	assert.EqualValues(t, 0, job.Fails)
	assert.Equal(t, "", job.LastErr)
	assert.EqualValues(t, 0, job.FailedAt)
}

func TestClientRetryAllDeadJobsBig(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	conn := pool.Get()
	defer conn.Close()

	// Ok, we need to efficiently add 10k jobs to the dead queue.
	// I tried using insertDeadJob but it was too slow (increased test time by 1 second)
	dead := models.RedisKey2JobDead(ns)
	for i := 0; i < 10000; i++ {
		job := &task.Job{
			Name:       "wat1",
			ID:         utils.MakeIdentifier(),
			EnqueuedAt: 12345,
			Args:       nil,
			Fails:      3,
			LastErr:    "sorry",
			FailedAt:   12347,
		}

		rawJSON, _ := job.Serialize()
		conn.Send("ZADD", dead, 12347, rawJSON)
	}
	err := conn.Flush()
	assert.NoError(t, err)

	if _, err := conn.Do("SADD", models.RedisKey2ValidJobs(ns), "wat1"); err != nil {
		panic(err)
	}

	// Add a dead task with a non-existent queue:
	job := &task.Job{
		Name:       "dontexist",
		ID:         utils.MakeIdentifier(),
		EnqueuedAt: 12345,
		Args:       nil,
		Fails:      3,
		LastErr:    "sorry",
		FailedAt:   12347,
	}

	rawJSON, _ := job.Serialize()

	_, err = conn.Do("ZADD", dead, 12347, rawJSON)
	if err != nil {
		panic(err.Error())
	}

	client := NewClient(ctx, ns)
	_, count, err := client.DeadJobs(1)
	assert.NoError(t, err)
	assert.EqualValues(t, 10001, count)

	err = client.RetryAllDeadJobs()
	assert.NoError(t, err)
	_, count, err = client.DeadJobs(1)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, count) // the funny task that we didn't know how to queue up

	jobCount := helper.ListSize(pool, models.RedisKey2Job(ns, "wat1"))
	assert.EqualValues(t, 10000, jobCount)

	_, job = helper.JobOnZset(pool, dead)
	assert.Equal(t, "dontexist", job.Name)
	assert.Equal(t, "unknown task when requeueing", job.LastErr)
}

func TestClientDeleteScheduledJob(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	// Delete an invalid task. Make sure we get error
	client := NewClient(ctx, ns)
	err := client.DeleteScheduledJob(3, "bob")
	assert.Equal(t, ErrNotDeleted, err)

	// Schedule a task. Delete it.
	enq := enqueue.NewEnqueuer(ctx, ns, "", "")
	j, err := enq.EnqueueIn("foo", 10, nil)
	assert.NoError(t, err)
	assert.NotNil(t, j)

	err = client.DeleteScheduledJob(j.RunAt, j.ID)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, helper.ZsetSize(pool, models.RedisKey2JobScheduled(ns)))
}

func TestClientDeleteScheduledUniqueJob(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	// Schedule a unique task. Delete it. Ensure we can component it again.
	enq := enqueue.NewEnqueuer(ctx, ns, "", "")
	j, err := enq.EnqueueUniqueIn("foo", 10, nil)
	assert.NoError(t, err)
	assert.NotNil(t, j)

	client := NewClient(ctx, ns)
	err = client.DeleteScheduledJob(j.RunAt, j.ID)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, helper.ZsetSize(pool, models.RedisKey2JobScheduled(ns)))

	j, err = enq.EnqueueUniqueIn("foo", 10, nil) // Can do it again
	assert.NoError(t, err)
	assert.NotNil(t, j) // Nil? We didn't clear the unique task signature.
}

func TestClientDeleteRetryJob(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	utils.SetNowEpochSecondsMock(1425263409)
	defer utils.ResetNowEpochSecondsMock()

	enqueuer := enqueue.NewEnqueuer(ctx, ns, "", "")
	j, err := enqueuer.Enqueue("wat", task.Q{"a": 1, "b": 2})
	assert.Nil(t, err)

	utils.SetNowEpochSecondsMock(1425263429)

	wp := pool2.NewWorkerPool(ctx)
	wp.Job("wat", func(job *task.Job) error {
		return fmt.Errorf("ohno")
	})
	wp.Start()
	wp.Drain()
	wp.Stop()

	// Ok so now we have a retry task
	client := NewClient(ctx, ns)
	jobs, count, err := client.RetryJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))
	if assert.EqualValues(t, 1, count) {
		err = client.DeleteRetryJob(jobs[0].RetryAt, j.ID)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, helper.ZsetSize(pool, models.RedisKey2JobRetry(ns)))
	}
}

func insertDeadJob(ns string, pool *redis.Pool, name string, encAt, failAt int64) *task.Job {
	job := &task.Job{
		Name:       name,
		ID:         utils.MakeIdentifier(),
		EnqueuedAt: encAt,
		Args:       nil,
		Fails:      3,
		LastErr:    "sorry",
		FailedAt:   failAt,
	}

	rawJSON, _ := job.Serialize()

	conn := pool.Get()
	defer conn.Close()
	_, err := conn.Do("ZADD", models.RedisKey2JobDead(ns), failAt, rawJSON)
	if err != nil {
		panic(err.Error())
	}

	if _, err := conn.Do("SADD", models.RedisKey2ValidJobs(ns), name); err != nil {
		panic(err)
	}

	return job
}

func getQueuedJob(ns string, pool *redis.Pool, name string) *task.Job {
	conn := pool.Get()
	defer conn.Close()
	jobBytes, err := redis.Bytes(conn.Do("RPOP", models.RedisKey2JobPrefix(ns)+name))
	if err != nil {
		return nil
	}

	job, err := task.NewJob(jobBytes, nil, nil)
	if err != nil {
		return nil
	}
	return job
}
