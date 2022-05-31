package reape

import (
	"fmt"
	"github.com/dhenisdj/scheduler/component/common/models"
	"github.com/dhenisdj/scheduler/component/context"
	"github.com/dhenisdj/scheduler/config"
	"math/rand"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
)

const (
	deadTime       = 10 * time.Second // 2 x heartbeat
	reapPeriod     = 10 * time.Minute
	reapJitterSecs = 30
)

type DeadPoolReaper struct {
	namespace   string
	ctx         context.Context
	deadTime    time.Duration
	reapPeriod  time.Duration
	curJobTypes []string

	stopChan         chan struct{}
	doneStoppingChan chan struct{}
}

type deadPoolInfo struct {
	jobTypes  []string
	workerIds []string
}

func NewDeadPoolReaper(ctx context.Context, namespace string, curJobTypes []string) *DeadPoolReaper {
	return &DeadPoolReaper{
		namespace:        namespace,
		ctx:              ctx,
		deadTime:         deadTime,
		reapPeriod:       reapPeriod,
		curJobTypes:      curJobTypes,
		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),
	}
}

func (r *DeadPoolReaper) Start() {
	go r.loop()
}

func (r *DeadPoolReaper) Stop() {
	r.stopChan <- struct{}{}
	<-r.doneStoppingChan
}

func (r *DeadPoolReaper) loop() {
	// Reap immediately after we provide some time for initialization
	timer := time.NewTimer(r.deadTime)
	defer timer.Stop()

	for {
		select {
		case <-r.stopChan:
			r.doneStoppingChan <- struct{}{}
			return
		case <-timer.C:
			// Schedule next occurrence periodically with jitter
			timer.Reset(r.reapPeriod + time.Duration(rand.Intn(reapJitterSecs))*time.Second)

			// Reap
			if err := r.reap(); err != nil {
				r.ctx.LE("dead_pool_reaper.reap", err)
			}
		}
	}
}

func (r *DeadPoolReaper) reap() error {
	// Get dead pools
	deadPoolIDs, err := r.findDeadPools()
	if err != nil {
		return err
	}

	conn := r.ctx.Redis().Get()
	defer conn.Close()

	workerPoolsKey := models.RedisKey2Pools(r.namespace)

	// Cleanup all dead pools
	for deadPoolID, deadInfo := range deadPoolIDs {
		lockJobTypes := deadInfo.jobTypes
		// if we found jobs from the heartbeat, requeue them and remove the heartbeat
		if len(deadInfo.jobTypes) > 0 {
			r.requeueInProgressJobs(deadPoolID, deadInfo.jobTypes)
			if _, err = conn.Do("DEL", models.RedisKey2Heartbeat(r.namespace, deadPoolID)); err != nil {
				return err
			}
		} else {
			// try to clean up locks for the current set of jobs if heartbeat was not found
			lockJobTypes = r.curJobTypes
		}
		// Remove dead pool from work pools set
		if _, err = conn.Do("SREM", workerPoolsKey, deadPoolID); err != nil {
			return err
		}
		// Cleanup any stale lock info
		if err = r.cleanStaleLockInfo(deadPoolID, lockJobTypes); err != nil {
			return err
		}
		// Cleanup dead observers for dead workers only in workerPool
		if len(deadInfo.workerIds) > 0 {
			if err = r.cleanDeadObservers(deadInfo.workerIds); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *DeadPoolReaper) cleanDeadObservers(workerIds []string) error {
	conn := r.ctx.Redis().Get()
	for _, workerId := range workerIds {
		conn.Send("DEL", models.RedisKey2Observation(r.ctx.CONF().NameSpace, workerId))
	}
	if err := conn.Flush(); err != nil {
		r.ctx.Wf("dead observers for dead workers clean failed")
		return err
	}
	return nil
}

func (r *DeadPoolReaper) cleanStaleLockInfo(poolID string, jobTypes []string) error {
	numKeys := len(jobTypes) * 2
	redisReapLocksScript := redis.NewScript(numKeys, models.RedisLuaReapStaleLocks)
	var scriptArgs = make([]interface{}, 0, numKeys+1) // +1 for argv[1]

	for _, jobType := range jobTypes {
		scriptArgs = append(scriptArgs, models.RedisKey2JobLock(r.namespace, jobType), models.RedisKey2JobLockInfo(r.namespace, jobType))
	}
	scriptArgs = append(scriptArgs, poolID) // ARGV[1]

	conn := r.ctx.Redis().Get()
	defer conn.Close()
	if _, err := redisReapLocksScript.Do(conn, scriptArgs...); err != nil {
		return err
	}

	return nil
}

func (r *DeadPoolReaper) requeueInProgressJobs(poolID string, jobTypes []string) error {
	numKeys := len(jobTypes) * config.RequeueKeysPerJob
	redisRequeueScript := redis.NewScript(numKeys, models.RedisLuaReenqueueJob)
	var scriptArgs = make([]interface{}, 0, numKeys+1)

	for _, jobType := range jobTypes {
		// pops from in progress, push into task queue and decrement the queue lock
		scriptArgs = append(scriptArgs, models.RedisKey2JobInProgress(r.namespace, poolID, jobType), models.RedisKey2Job(r.namespace, jobType), models.RedisKey2JobLock(r.namespace, jobType), models.RedisKey2JobLockInfo(r.namespace, jobType)) // KEYS[1-4 * N]
	}
	scriptArgs = append(scriptArgs, poolID) // ARGV[1]

	conn := r.ctx.Redis().Get()
	defer conn.Close()

	// Keep moving jobs until all queues are empty
	for {
		values, err := redis.Values(redisRequeueScript.Do(conn, scriptArgs...))
		if err == redis.ErrNil {
			return nil
		} else if err != nil {
			return err
		}

		if len(values) != 3 {
			return fmt.Errorf("need 3 elements back")
		}
	}
}

func (r *DeadPoolReaper) findDeadPools() (map[string]*deadPoolInfo, error) {
	conn := r.ctx.Redis().Get()

	defer conn.Close()

	workerPoolsKey := models.RedisKey2Pools(r.namespace)

	workerPoolIDs, err := redis.Strings(conn.Do("SMEMBERS", workerPoolsKey))
	if err != nil {
		return nil, err
	}

	deadPools := map[string]*deadPoolInfo{}
	for _, workerPoolID := range workerPoolIDs {
		heartbeatKey := models.RedisKey2Heartbeat(r.namespace, workerPoolID)
		heartbeatAt, err := redis.Int64(conn.Do("HGET", heartbeatKey, config.HeartbeatBeat))
		if err == redis.ErrNil {
			// heartbeat expired, save dead pool and use cur set of jobs from reaper
			deadPools[workerPoolID] = &deadPoolInfo{}
			continue
		}
		if err != nil {
			return nil, err
		}

		// Check that last heartbeat was long enough ago to consider the pool dead
		if time.Unix(heartbeatAt, 0).Add(r.deadTime).After(time.Now()) {
			continue
		}

		deadInfo := &deadPoolInfo{}

		jobTypesList, err := redis.String(conn.Do("HGET", heartbeatKey, config.HeartbeatJobNames))
		if err == redis.ErrNil {
			continue
		}
		if err != nil {
			return nil, err
		}
		deadInfo.jobTypes = strings.Split(jobTypesList, ",")

		kind, err := redis.String(conn.Do("HGET", heartbeatKey, config.HeartbeatKind))
		if err == redis.ErrNil {
			continue
		}
		if err != nil {
			return nil, err
		}
		if kind == config.PoolKindWorker {
			workerIds, err := redis.String(conn.Do("HGET", heartbeatKey, config.HeartbeatExecutorIds))
			r.ctx.If("dead worker ids %s found for %s", workerIds, heartbeatKey)
			if err == redis.ErrNil {
				continue
			}
			if err != nil {
				return nil, err
			}
			deadInfo.workerIds = strings.Split(workerIds, ",")
		}

		deadPools[workerPoolID] = deadInfo
	}

	return deadPools, nil
}
