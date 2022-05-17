package pool

import (
	"encoding/json"
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/execute/group"
	"github.com/dhenisdj/scheduler/component/actors/execute/work"
	"github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/common/context"
	"github.com/dhenisdj/scheduler/component/common/entities"
	"github.com/dhenisdj/scheduler/component/handler"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/dhenisdj/scheduler/config"
	"github.com/gomodule/redigo/redis"
	"sort"
	"strings"
	"sync"
)

// WorkerPool represents a pool of workers. It forms the primary API of gocraft/work. WorkerPools provide the public API of gocraft/work. You can attach jobs and middlware to them. You can start and stop them. Based on their concurrency setting, they'll spin up N work goroutines.
type WorkerPool struct {
	*Pool
	WorkerGroup map[string]*work.WorkerGroup // execute:business -> workerGroup
}

// NewWorkerPool creates a new work pool as per the NewWorkerPool function, but permits you to specify
// additional options such as sleep backoffs.
func NewWorkerPool(ctx *context.Context, configuration *entities.Configuration, pool *redis.Pool) *WorkerPool {
	if pool == nil {
		panic("NewWorkerPool needs a non-nil *redi.Pool")
	}

	executorConfig := configuration.Spark.Executor
	sparkConf := configuration.Spark.SparkConf
	wp := &WorkerPool{
		Pool: newPool(
			configuration.Env,
			config.SchedulerNamespace,
			config.PoolKindWorker,
			WithContext(ctx),
			WithBackoffs(config.SleepBackoffsInMilliseconds),
			WithRedis(pool),
		),
		WorkerGroup: make(map[string]*work.WorkerGroup),
	}

	for _, executor := range *executorConfig {
		executorName := executor.Name
		business := executor.Business
		if !business.Valid() {
			panic("NewWorkerPool configuration file error for business")
		}
		session := executor.Session
		if !session.Valid() {
			panic("NewWorkerPool configuration file error for session")
		}
		bizConcurrency := business.Concurrency

		for biz, concurrency := range bizConcurrency {

			wg := work.NewWorkerGroup(
				wp.env,
				wp.Namespace,
				wp.PoolID,
				group.WithContext(ctx),
				group.WithRedis(pool),
			)

			if executorName == "SyncDistributionCommon" && biz == "ads_ops" {
				biz = "REFERRAL"
			}

			id := utils.MakeIdentifier()
			if session.IsBatch {
				workerId := fmt.Sprintf("%s.%d", id, 0)
				worker := work.NewWorker(
					wg.Namespace,
					workerId,
					wg.GroupID,
					wp.PoolID,
					work.WithContext(ctx),
					work.WithBatchSession(biz, session, business, sparkConf),
					work.WithRedis(wp.Redis),
					work.WithBackoffs(wp.sleepBackoffs),
				)
				wg.Workers = append(wg.Workers, worker)
			} else {
				for i := 0; i < concurrency; i++ {
					workerId := fmt.Sprintf("%s.%d", id, i)
					worker := work.NewWorker(
						wg.Namespace,
						workerId,
						wg.GroupID,
						wp.PoolID,
						work.WithContext(ctx),
						work.WithSingleSession(),
						work.WithRedis(wp.Redis),
						work.WithBackoffs(wp.sleepBackoffs),
					)
					wg.Workers = append(wg.Workers, worker)
				}
			}

			groupJobName := fmt.Sprintf("%s%s", executorName, strings.ToUpper(biz))

			//Build task types by executor and biz
			wp.JobWithOptions(
				groupJobName,
				task.JobOptions{
					IsBatch:        session.IsBatch,
					SkipDead:       true,
					MaxConcurrency: uint(len(wg.Workers)),
					//Backoff:        task.DefaultBackoffCalculator, This is the default calculator if not set
				},
				handler.Handlers[session.IsBatch])

			wp.WorkerGroup[groupJobName] = wg
		}

	}

	return wp
}

func (wp *WorkerPool) Middleware(fn interface{}) *WorkerPool {
	wp.middleware(fn)

	for k, wg := range wp.WorkerGroup {
		for _, w := range wg.Workers {
			w.UpdateMiddlewareAndJobTypes(wp.middlewares, map[string]*task.JobType{k: wp.jobTypes[k]})
		}
	}

	return wp
}

func (wp *WorkerPool) Job(name string, fn interface{}) *Pool {
	return wp.job(name, fn)
}

func (wp *WorkerPool) JobWithOptions(name string, jobOpts task.JobOptions, fn interface{}) *WorkerPool {
	wp.jobWithOptions(name, jobOpts, fn)

	for _, wg := range wp.WorkerGroup {
		for _, w := range wg.Workers {
			w.UpdateMiddlewareAndJobTypes(wp.middlewares, map[string]*task.JobType{name: wp.jobTypes[name]})
		}
	}

	return wp
}

// Start starts the workers and associated processes.
func (wp *WorkerPool) Start() {
	concurrences := make(map[string]int)
	for k, wg := range wp.WorkerGroup {
		currCon := len(wg.Workers)
		concurrences[k] = currCon
		go wg.Start()
	}
	concurrencyMap, _ := json.Marshal(concurrences)

	wp.start(concurrencyMap, wp.WorkerIDs())
}

// Stop stops the workers and associated processes.
func (wp *WorkerPool) Stop() {
	if !wp.started {
		return
	}
	wp.started = false

	wg := sync.WaitGroup{}
	for _, wkg := range wp.WorkerGroup {
		wg.Add(1)
		go func(wkg *work.WorkerGroup) {
			wkg.Stop()
			wg.Done()
		}(wkg)
	}
	wg.Wait()
	wp.stop()
}

// Drain drains all jobs in the queue before returning. Note that if jobs are added faster than we can process them, this function wouldn't return.
func (wp *WorkerPool) Drain() {
	wg := sync.WaitGroup{}
	for _, wkg := range wp.WorkerGroup {
		wg.Add(1)
		go func(wkg *work.WorkerGroup) {
			wkg.Drain()
			wg.Done()
		}(wkg)
	}
	wg.Wait()

}

func (wp *WorkerPool) WorkerIDs() []string {
	l := 0
	for _, wkg := range wp.WorkerGroup {
		l += len(wkg.Workers)
	}
	wids := make([]string, 0, l)
	for _, wkg := range wp.WorkerGroup {
		for _, w := range wkg.Workers {
			wids = append(wids, w.WorkerID)
		}
	}
	sort.Strings(wids)
	return wids
}
