package reape

import (
	"encoding/json"
	pool2 "github.com/dhenisdj/scheduler/component/actors/pool"
	"github.com/dhenisdj/scheduler/component/actors/pool/heartbeat"
	"github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/context"
	"github.com/dhenisdj/scheduler/component/utils/helper"
	"github.com/dhenisdj/scheduler/config"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestDeadPoolReaper(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	conn := pool.Get()
	defer conn.Close()

	workerPoolsKey := models.RedisKey2Pools(ns)

	// Create redi data
	var err error
	err = conn.Send("SADD", workerPoolsKey, "1")
	assert.NoError(t, err)
	err = conn.Send("SADD", workerPoolsKey, "2")
	assert.NoError(t, err)
	err = conn.Send("SADD", workerPoolsKey, "3")
	assert.NoError(t, err)

	err = conn.Send("HMSET", models.RedisKey2Heartbeat(ns, "1"),
		"heartbeat_at", time.Now().Unix(),
		"job_names", "type1,type2",
	)
	assert.NoError(t, err)

	err = conn.Send("HMSET", models.RedisKey2Heartbeat(ns, "2"),
		"heartbeat_at", time.Now().Add(-1*time.Hour).Unix(),
		"job_names", "type1,type2",
	)
	assert.NoError(t, err)

	err = conn.Send("HMSET", models.RedisKey2Heartbeat(ns, "3"),
		"heartbeat_at", time.Now().Add(-1*time.Hour).Unix(),
		"job_names", "type1,type2",
	)
	assert.NoError(t, err)
	err = conn.Flush()
	assert.NoError(t, err)

	// Test getting dead pool
	reaper := NewDeadPoolReaper(context.New("test_sg", true), ns, []string{})
	deadPools, err := reaper.findDeadPools()
	assert.NoError(t, err)

	assert.Equal(t, map[string]*deadPoolInfo{"2": {[]string{"type1", "type2"}, []string{}}, "3": {[]string{"type1", "type2"}, []string{}}}, deadPools)

	// Test requeueing jobs
	_, err = conn.Do("lpush", models.RedisKey2JobInProgress(ns, "2", "type1"), "foo")
	assert.NoError(t, err)
	_, err = conn.Do("incr", models.RedisKey2JobLock(ns, "type1"))
	assert.NoError(t, err)
	_, err = conn.Do("hincrby", models.RedisKey2JobLockInfo(ns, "type1"), "2", 1) // work pool 2 has lock
	assert.NoError(t, err)

	// Ensure 0 jobs in jobs queue
	jobsCount, err := redis.Int(conn.Do("llen", models.RedisKey2Job(ns, "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 0, jobsCount)

	// Ensure 1 task in inprogress queue
	jobsCount, err = redis.Int(conn.Do("llen", models.RedisKey2JobInProgress(ns, "2", "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 1, jobsCount)

	// Reap
	err = reaper.reap()
	assert.NoError(t, err)

	// Ensure 1 jobs in jobs queue
	jobsCount, err = redis.Int(conn.Do("llen", models.RedisKey2Job(ns, "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 1, jobsCount)

	// Ensure 0 task in inprogress queue
	jobsCount, err = redis.Int(conn.Do("llen", models.RedisKey2JobInProgress(ns, "2", "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 0, jobsCount)

	// Locks should get cleaned up
	assert.EqualValues(t, 0, helper.GetInt64(pool, models.RedisKey2JobLock(ns, "type1")))
	v, _ := conn.Do("HGET", models.RedisKey2JobLockInfo(ns, "type1"), "2")
	assert.Nil(t, v)
}

func TestDeadPoolReaperNoHeartbeat(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace

	conn := pool.Get()
	defer conn.Close()

	workerPoolsKey := models.RedisKey2Pools(ns)

	// Create redi data
	var err error
	helper.CleanKeyspace(ns, pool)
	err = conn.Send("SADD", workerPoolsKey, "1")
	assert.NoError(t, err)
	err = conn.Send("SADD", workerPoolsKey, "2")
	assert.NoError(t, err)
	err = conn.Send("SADD", workerPoolsKey, "3")
	assert.NoError(t, err)
	// stale lock info
	err = conn.Send("SET", models.RedisKey2JobLock(ns, "type1"), 3)
	assert.NoError(t, err)
	err = conn.Send("HSET", models.RedisKey2JobLockInfo(ns, "type1"), "1", 1)
	assert.NoError(t, err)
	err = conn.Send("HSET", models.RedisKey2JobLockInfo(ns, "type1"), "2", 1)
	assert.NoError(t, err)
	err = conn.Send("HSET", models.RedisKey2JobLockInfo(ns, "type1"), "3", 1)
	assert.NoError(t, err)
	err = conn.Flush()
	assert.NoError(t, err)

	// make sure test data was created
	numPools, err := redis.Int(conn.Do("scard", workerPoolsKey))
	assert.NoError(t, err)
	assert.EqualValues(t, 3, numPools)

	// Test getting dead pool ids
	reaper := NewDeadPoolReaper(context.New("test_sg", true), ns, []string{"type1"})
	deadPools, err := reaper.findDeadPools()
	assert.NoError(t, err)
	assert.Equal(t, map[string]*deadPoolInfo{"1": {}, "2": {}, "3": {}}, deadPools)

	// Test requeueing jobs
	_, err = conn.Do("lpush", models.RedisKey2JobInProgress(ns, "2", "type1"), "foo")
	assert.NoError(t, err)

	// Ensure 0 jobs in jobs queue
	jobsCount, err := redis.Int(conn.Do("llen", models.RedisKey2Job(ns, "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 0, jobsCount)

	// Ensure 1 task in inprogress queue
	jobsCount, err = redis.Int(conn.Do("llen", models.RedisKey2JobInProgress(ns, "2", "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 1, jobsCount)

	// Ensure dead work pools still in the set
	jobsCount, err = redis.Int(conn.Do("scard", models.RedisKey2Pools(ns)))
	assert.NoError(t, err)
	assert.Equal(t, 3, jobsCount)

	// Reap
	err = reaper.reap()
	assert.NoError(t, err)

	// Ensure jobs queue was not altered
	jobsCount, err = redis.Int(conn.Do("llen", models.RedisKey2Job(ns, "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 0, jobsCount)

	// Ensure inprogress queue was not altered
	jobsCount, err = redis.Int(conn.Do("llen", models.RedisKey2JobInProgress(ns, "2", "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 1, jobsCount)

	// Ensure dead work pools were removed from the set
	jobsCount, err = redis.Int(conn.Do("scard", models.RedisKey2Pools(ns)))
	assert.NoError(t, err)
	assert.Equal(t, 0, jobsCount)

	// Stale lock info was cleaned up using reap.curJobTypes
	assert.EqualValues(t, 0, helper.GetInt64(pool, models.RedisKey2JobLock(ns, "type1")))
	for _, poolID := range []string{"1", "2", "3"} {
		v, _ := conn.Do("HGET", models.RedisKey2JobLockInfo(ns, "type1"), poolID)
		assert.Nil(t, v)
	}
}

func TestDeadPoolReaperNoJobTypes(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	conn := pool.Get()
	defer conn.Close()

	workerPoolsKey := models.RedisKey2Pools(ns)

	// Create redi data
	var err error
	err = conn.Send("SADD", workerPoolsKey, "1")
	assert.NoError(t, err)
	err = conn.Send("SADD", workerPoolsKey, "2")
	assert.NoError(t, err)

	err = conn.Send("HMSET", models.RedisKey2Heartbeat(ns, "1"),
		"heartbeat_at", time.Now().Add(-1*time.Hour).Unix(),
	)
	assert.NoError(t, err)

	err = conn.Send("HMSET", models.RedisKey2Heartbeat(ns, "2"),
		"heartbeat_at", time.Now().Add(-1*time.Hour).Unix(),
		"job_names", "type1,type2",
	)
	assert.NoError(t, err)

	err = conn.Flush()
	assert.NoError(t, err)

	// Test getting dead pool
	reaper := NewDeadPoolReaper(context.New("test_sg", true), ns, []string{})
	deadPools, err := reaper.findDeadPools()
	assert.NoError(t, err)
	assert.Equal(t, map[string]*deadPoolInfo{"2": {[]string{"type1", "type2"}, []string{}}}, deadPools)

	// Test requeueing jobs
	_, err = conn.Do("lpush", models.RedisKey2JobInProgress(ns, "1", "type1"), "foo")
	assert.NoError(t, err)
	_, err = conn.Do("lpush", models.RedisKey2JobInProgress(ns, "2", "type1"), "foo")
	assert.NoError(t, err)

	// Ensure 0 jobs in jobs queue
	jobsCount, err := redis.Int(conn.Do("llen", models.RedisKey2Job(ns, "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 0, jobsCount)

	// Ensure 1 task in inprogress queue for each task
	jobsCount, err = redis.Int(conn.Do("llen", models.RedisKey2JobInProgress(ns, "1", "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 1, jobsCount)
	jobsCount, err = redis.Int(conn.Do("llen", models.RedisKey2JobInProgress(ns, "2", "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 1, jobsCount)

	// Reap. Ensure task 2 is requeued but not task 1
	err = reaper.reap()
	assert.NoError(t, err)

	// Ensure 1 jobs in jobs queue
	jobsCount, err = redis.Int(conn.Do("llen", models.RedisKey2Job(ns, "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 1, jobsCount)

	// Ensure 1 task in inprogress queue for 1
	jobsCount, err = redis.Int(conn.Do("llen", models.RedisKey2JobInProgress(ns, "1", "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 1, jobsCount)

	// Ensure 0 jobs in inprogress queue for 2
	jobsCount, err = redis.Int(conn.Do("llen", models.RedisKey2JobInProgress(ns, "2", "type1")))
	assert.NoError(t, err)
	assert.Equal(t, 0, jobsCount)
}

func TestDeadPoolReaperWithWorkerPools(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	job1 := "job1"
	stalePoolID := "aaa"
	helper.CleanKeyspace(ns, pool)
	// test vars
	expectedDeadTime := 5 * time.Millisecond

	// create a stale task with a heartbeat
	conn := pool.Get()
	defer conn.Close()
	_, err := conn.Do("SADD", models.RedisKey2Pools(ns), stalePoolID)
	assert.NoError(t, err)
	_, err = conn.Do("LPUSH", models.RedisKey2JobInProgress(ns, stalePoolID, job1), `{"sleep": 10}`)
	assert.NoError(t, err)
	jobTypes := map[string]*task.JobType{"job1": nil}
	concurrences, _ := json.Marshal(map[string]int{"job1": 1})
	staleHeart := heartbeat.NewPoolHeartbeater(context.New("test_sg", true), ns, "", stalePoolID, jobTypes, concurrences, []string{"id1"})
	staleHeart.Start()

	// should have 1 stale task and empty task queue
	assert.EqualValues(t, 1, helper.ListSize(pool, models.RedisKey2JobInProgress(ns, stalePoolID, job1)))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2Job(ns, job1)))

	// setup a work pool and start the reaper, which should restart the stale task above
	wp := setupTestWorkerPool(pool, ns, job1, task.JobOptions{Priority: 1})
	wp.DeadPoolReaper = NewDeadPoolReaper(context.New("test_sg", true), wp.Namespace, []string{"job1"})
	// sleep long enough for staleJob to be considered dead
	time.Sleep(expectedDeadTime * 2)

	// now we should have 1 task in queue and no more stale jobs
	assert.EqualValues(t, 1, helper.ListSize(pool, models.RedisKey2Job(ns, job1)))
	assert.EqualValues(t, 0, helper.ListSize(pool, models.RedisKey2JobInProgress(ns, wp.PoolID, job1)))
	staleHeart.Stop()
	wp.DeadPoolReaper.Stop()
}

func TestDeadPoolReaperCleanStaleLocks(t *testing.T) {
	pool := helper.NewTestPool(":6379")
	ns := config.SchedulerNamespace
	helper.CleanKeyspace(ns, pool)

	conn := pool.Get()
	defer conn.Close()
	job1, job2 := "type1", "type2"
	jobNames := []string{job1, job2}
	workerPoolID1, workerPoolID2 := "1", "2"
	lock1 := models.RedisKey2JobLock(ns, job1)
	lock2 := models.RedisKey2JobLock(ns, job2)
	lockInfo1 := models.RedisKey2JobLockInfo(ns, job1)
	lockInfo2 := models.RedisKey2JobLockInfo(ns, job2)

	// Create redi data
	var err error
	err = conn.Send("SET", lock1, 3)
	assert.NoError(t, err)
	err = conn.Send("SET", lock2, 1)
	assert.NoError(t, err)
	err = conn.Send("HSET", lockInfo1, workerPoolID1, 1) // workerPoolID1 holds 1 lock on job1
	assert.NoError(t, err)
	err = conn.Send("HSET", lockInfo1, workerPoolID2, 2) // workerPoolID2 holds 2 locks on job1
	assert.NoError(t, err)
	err = conn.Send("HSET", lockInfo2, workerPoolID2, 2) // test that we don't go below 0 on job2 lock
	assert.NoError(t, err)
	err = conn.Flush()
	assert.NoError(t, err)

	reaper := NewDeadPoolReaper(context.New("test_sg", true), ns, jobNames)
	// clean lock info for workerPoolID1
	reaper.cleanStaleLockInfo(workerPoolID1, jobNames)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, helper.GetInt64(pool, lock1)) // job1 lock should be decr by 1
	assert.EqualValues(t, 1, helper.GetInt64(pool, lock2)) // job2 lock is unchanged
	v, _ := conn.Do("HGET", lockInfo1, workerPoolID1)      // workerPoolID1 removed from job1's lock info
	assert.Nil(t, v)

	// now clean lock info for workerPoolID2
	reaper.cleanStaleLockInfo(workerPoolID2, jobNames)
	assert.NoError(t, err)
	// both locks should be at 0
	assert.EqualValues(t, 0, helper.GetInt64(pool, lock1))
	assert.EqualValues(t, 0, helper.GetInt64(pool, lock2))
	// work pool ID 2 removed from both lock info hashes
	v, err = conn.Do("HGET", lockInfo1, workerPoolID2)
	assert.Nil(t, v)
	v, err = conn.Do("HGET", lockInfo2, workerPoolID2)
	assert.Nil(t, v)
}

func setupTestWorkerPool(pool *redis.Pool, namespace, jobName string, jobOpts task.JobOptions) *pool2.WorkerPool {
	helper.DeleteQueue(pool, namespace, jobName)
	helper.DeleteRetryAndDead(pool, namespace)
	helper.DeletePausedAndLockedKeys(namespace, jobName, pool)
	wp := pool2.NewWorkerPool(context.New("test_sg", true))
	wp.JobWithOptions(jobName, jobOpts, (*helper.TestContext).SleepyJob)
	// reset the backoff times to help with testing
	config.SleepBackoffsInMilliseconds = []int64{10, 10, 10, 10, 10}
	return wp
}
