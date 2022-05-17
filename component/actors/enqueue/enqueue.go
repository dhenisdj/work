package enqueue

import (
	"fmt"
	"github.com/dhenisdj/scheduler/client"
	"github.com/dhenisdj/scheduler/component/actors/task"
	context "github.com/dhenisdj/scheduler/component/common/context"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/utils"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Enqueuer can enqueue jobs.
type Enqueuer struct {
	Namespace string // eg, "an"
	Executor  string // eg, "SyncAudienceRecordHive"
	Biz       string // eg, "crm"
	Pool      *redis.Pool
	ctx       *context.Context

	queuePrefix           string // eg, "myapp-work:jobs:"
	knownJobs             map[string]int64
	enqueueUniqueScript   *redis.Script
	enqueueUniqueInScript *redis.Script
	mtx                   sync.RWMutex
}

// NewEnqueuer creates a new enqueuer with the specified Redis namespace and Redis pool.
func NewEnqueuer(ctx *context.Context, namespace, executor, biz string, pool *redis.Pool) *Enqueuer {
	if pool == nil {
		panic("NewEnqueuer needs a non-nil *redi.Pool")
	}

	return &Enqueuer{
		Namespace:             namespace,
		Executor:              executor,
		Biz:                   biz,
		Pool:                  pool,
		ctx:                   ctx,
		queuePrefix:           models.RedisKey2JobPrefix(namespace),
		knownJobs:             make(map[string]int64),
		enqueueUniqueScript:   redis.NewScript(2, models.RedisLuaEnqueueUnique),
		enqueueUniqueInScript: redis.NewScript(2, models.RedisLuaEnqueueUniqueIn),
	}
}

func (e *Enqueuer) EnqueueSparkTask(job *task.Job) *task.Job {
	var runErr error
	jobName := job.Name
	rawJSON, err := job.Serialize()
	if err != nil {
		runErr = fmt.Errorf("serialize task to raw json error")
		e.ctx.LE("enqueue task ", runErr)
	}

	conn := e.Pool.Get()
	defer conn.Close()

	if _, err := conn.Do("LPUSH", e.queuePrefix+jobName, rawJSON); err != nil {
		runErr = fmt.Errorf("lpush task to redi error")
		e.ctx.LE("enqueue task ", runErr)
	}

	if err := e.addToKnownJobs(conn, jobName); err != nil {
		runErr = fmt.Errorf("add to known jobs error")
		e.ctx.LE("enqueue task ", runErr)
	}

	return job
}

// Enqueue will enqueue the specified task name and arguments. The args param can be nil if no args ar needed.
// Example: e.Enqueue("send_email", work.Q{"addr": "test@example.com"})
func (e *Enqueuer) Enqueue(jobName string, args map[string]interface{}) (*task.Job, error) {
	job := &task.Job{
		Name:       jobName,
		ID:         utils.MakeIdentifier(),
		EnqueuedAt: utils.NowEpochSeconds(),
		Args:       args,
	}

	rawJSON, err := job.Serialize()
	if err != nil {
		return nil, err
	}

	conn := e.Pool.Get()
	defer conn.Close()

	if _, err := conn.Do("LPUSH", e.queuePrefix+jobName, rawJSON); err != nil {
		return nil, err
	}

	if err := e.addToKnownJobs(conn, jobName); err != nil {
		return job, err
	}

	return job, nil
}

// EnqueueIn enqueues a task in the scheduled task queue for execution in secondsFromNow seconds.
func (e *Enqueuer) EnqueueIn(jobName string, secondsFromNow int64, args map[string]interface{}) (*client.ScheduledJob, error) {
	job := &task.Job{
		Name:       jobName,
		ID:         utils.MakeIdentifier(),
		EnqueuedAt: utils.NowEpochSeconds(),
		Args:       args,
	}

	rawJSON, err := job.Serialize()
	if err != nil {
		return nil, err
	}

	conn := e.Pool.Get()
	defer conn.Close()

	scheduledJob := &client.ScheduledJob{
		RunAt: utils.NowEpochSeconds() + secondsFromNow,
		Job:   job,
	}

	_, err = conn.Do("ZADD", models.RedisKey2JobScheduled(e.Namespace), scheduledJob.RunAt, rawJSON)
	if err != nil {
		return nil, err
	}

	if err := e.addToKnownJobs(conn, jobName); err != nil {
		return scheduledJob, err
	}

	return scheduledJob, nil
}

// EnqueueUnique enqueues a task unless a task is already enqueued with the same name and arguments.
// The already-enqueued task can be in the normal work queue or in the scheduled task queue.
// Once a work begins processing a task, another task with the same name and arguments can be enqueued again.
// Any failed jobs in the retry queue or dead queue don't count against the uniqueness -- so if a task fails and is retried, two unique jobs with the same name and arguments can be enqueued at once.
// In order to add robustness to the system, jobs are only unique for 24 hours after they're enqueued. This is mostly relevant for scheduled jobs.
// EnqueueUnique returns the task if it was enqueued and nil if it wasn't
func (e *Enqueuer) EnqueueUnique(jobName string, args map[string]interface{}) (*task.Job, error) {
	return e.EnqueueUniqueByKey(jobName, args, nil)
}

// EnqueueUniqueIn enqueues a unique task in the scheduled task queue for execution in secondsFromNow seconds. See EnqueueUnique for the semantics of unique jobs.
func (e *Enqueuer) EnqueueUniqueIn(jobName string, secondsFromNow int64, args map[string]interface{}) (*client.ScheduledJob, error) {
	return e.EnqueueUniqueInByKey(jobName, secondsFromNow, args, nil)
}

// EnqueueUniqueByKey enqueues a task unless a task is already enqueued with the same name and key, updating arguments.
// The already-enqueued task can be in the normal work queue or in the scheduled task queue.
// Once a work begins processing a task, another task with the same name and key can be enqueued again.
// Any failed jobs in the retry queue or dead queue don't count against the uniqueness -- so if a task fails and is retried, two unique jobs with the same name and arguments can be enqueued at once.
// In order to add robustness to the system, jobs are only unique for 24 hours after they're enqueued. This is mostly relevant for scheduled jobs.
// EnqueueUniqueByKey returns the task if it was enqueued and nil if it wasn't
func (e *Enqueuer) EnqueueUniqueByKey(jobName string, args map[string]interface{}, keyMap map[string]interface{}) (*task.Job, error) {
	enqueue, job, err := e.uniqueJobHelper(jobName, args, keyMap)
	if err != nil {
		return nil, err
	}

	res, err := enqueue(nil)

	if res == "ok" && err == nil {
		return job, nil
	}
	return nil, err
}

// EnqueueUniqueInByKey enqueues a task in the scheduled task queue that is unique on specified key for execution in secondsFromNow seconds. See EnqueueUnique for the semantics of unique jobs.
// Subsequent calls with same key will update arguments
func (e *Enqueuer) EnqueueUniqueInByKey(jobName string, secondsFromNow int64, args map[string]interface{}, keyMap map[string]interface{}) (*client.ScheduledJob, error) {
	enqueue, job, err := e.uniqueJobHelper(jobName, args, keyMap)
	if err != nil {
		return nil, err
	}

	scheduledJob := &client.ScheduledJob{
		RunAt: utils.NowEpochSeconds() + secondsFromNow,
		Job:   job,
	}

	res, err := enqueue(&scheduledJob.RunAt)
	if res == "ok" && err == nil {
		return scheduledJob, nil
	}
	return nil, err
}

func (e *Enqueuer) addToKnownJobs(conn redis.Conn, jobName string) error {
	needSadd := true
	now := time.Now().Unix()

	e.mtx.RLock()
	t, ok := e.knownJobs[jobName]
	e.mtx.RUnlock()

	if ok {
		if now < t {
			needSadd = false
		}
	}
	if needSadd {
		if _, err := conn.Do("SADD", models.RedisKey2ValidJobs(e.Namespace), jobName); err != nil {
			return err
		}

		e.mtx.Lock()
		e.knownJobs[jobName] = now + 300
		e.mtx.Unlock()
	}

	return nil
}

type enqueueFnType func(*int64) (string, error)

func (e *Enqueuer) uniqueJobHelper(jobName string, args map[string]interface{}, keyMap map[string]interface{}) (enqueueFnType, *task.Job, error) {
	useDefaultKeys := false
	if keyMap == nil {
		useDefaultKeys = true
		keyMap = args
	}

	uniqueKey, err := models.RedisKeyUniqueJob(e.Namespace, jobName, keyMap)
	if err != nil {
		return nil, nil, err
	}

	job := &task.Job{
		Name:       jobName,
		ID:         utils.MakeIdentifier(),
		EnqueuedAt: utils.NowEpochSeconds(),
		Args:       args,
		Unique:     true,
		UniqueKey:  uniqueKey,
	}

	rawJSON, err := job.Serialize()
	if err != nil {
		return nil, nil, err
	}

	enqueueFn := func(runAt *int64) (string, error) {
		conn := e.Pool.Get()
		defer conn.Close()

		if err := e.addToKnownJobs(conn, jobName); err != nil {
			return "", err
		}

		scriptArgs := []interface{}{}
		script := e.enqueueUniqueScript

		scriptArgs = append(scriptArgs, e.queuePrefix+jobName) // KEY[1]
		scriptArgs = append(scriptArgs, uniqueKey)             // KEY[2]
		scriptArgs = append(scriptArgs, rawJSON)               // ARGV[1]
		if useDefaultKeys {
			// keying on arguments so arguments can't be updated
			// we'll just get them off the original task so to save space, make this "1"
			scriptArgs = append(scriptArgs, "1") // ARGV[2]
		} else {
			// we'll use this for updated arguments since the task on the queue
			// doesn't get updated
			scriptArgs = append(scriptArgs, rawJSON) // ARGV[2]
		}

		if runAt != nil { // Scheduled task so different task queue with additional arg
			scriptArgs[0] = models.RedisKey2JobScheduled(e.Namespace) // KEY[1]
			scriptArgs = append(scriptArgs, *runAt)                   // ARGV[3]

			script = e.enqueueUniqueInScript
		}

		return redis.String(script.Do(conn, scriptArgs...))
	}

	return enqueueFn, job, nil
}
