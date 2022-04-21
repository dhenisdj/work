package work

import (
	"encoding/json"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"reflect"
	"sort"
	"sync"
)

// WorkerPool represents a pool of workers. It forms the primary API of gocraft/work. WorkerPools provide the public API of gocraft/work. You can attach jobs and middlware to them. You can start and stop them. Based on their concurrency setting, they'll spin up N worker goroutines.
type WorkerPool struct {
	*Pool
	WorkerGroup map[string]*WorkerGroup // executor:business -> workerGroup
}

// NewWorkerPool creates a new worker pool. ctx should be a struct literal whose type will be used for middleware and handlers.
// concurrency specifies how many workers to spin up - each worker can process jobs concurrently.
func NewWorkerPool(ctx interface{}, executorConfig Executor, pool *redis.Pool) *WorkerPool {
	return NewWorkerPoolWithOptions(ctx, executorConfig, pool, PoolOptions{})
}

// NewWorkerPoolWithOptions creates a new worker pool as per the NewWorkerPool function, but permits you to specify
// additional options such as sleep backoffs.
func NewWorkerPoolWithOptions(ctx interface{}, executorConfig Executor, pool *redis.Pool, poolOpts PoolOptions) *WorkerPool {
	if pool == nil {
		panic("NewWorkerPool needs a non-nil *redis.Pool")
	}

	ctxType := reflect.TypeOf(ctx)
	validateContextType(ctxType)
	wp := &WorkerPool{
		Pool:        newPoolWithOptions(ctx, WorkerPoolNamespace, PoolKindWorker, IdentifierTypeWorkerPool, pool, poolOpts),
		WorkerGroup: make(map[string]*WorkerGroup),
	}

	for _, executor := range executorConfig {
		executorName := executor.Name
		business := executor.Business
		if !business.validate() {
			panic("NewWorkerPool configuration file error for business")
		}
		bizConcurrency := business.Concurrency

		wg := &WorkerGroup{
			workerGroupID: makeIdentifier(IdentifierTypeWorkerGroup),
			namespace:     fmt.Sprintf("%s:%s", wp.namespace, executorName),
			poolID:        wp.poolID,
			workers:       make([]*worker, 0),
		}

		for biz, concurrency := range bizConcurrency {
			if executorName == "SyncDistributionCommon" && biz == "ads_ops" {
				biz = "REFERRAL"
			}

			ns := fmt.Sprintf("%s:%s", wg.namespace, biz)
			for i := 0; i < concurrency; i++ {
				worker := newWorker(ns, wp.poolID, wg.workerGroupID, wp.pool, wp.contextType, wp.sleepBackoffs)
				wg.workers = append(wg.workers, worker)
			}
		}
		wp.WorkerGroup[executorName] = wg

		isBatch := false
		if executor.Type == JobTypeBatch {
			isBatch = true
		}

		//Build job types by executor
		wp.JobWithOptions(
			executorName,
			JobOptions{
				IsBatch:        isBatch,
				SkipDead:       true,
				MaxConcurrency: uint(len(wp.WorkerGroup[executorName].workers)),
				Backoff:        defaultBackoffCalculator,
			},
			epsilonHandler)

	}

	return wp
}

func (wp *WorkerPool) Middleware(fn interface{}) *WorkerPool {
	wp.middleware(fn)

	for k, wg := range wp.WorkerGroup {
		for _, w := range wg.workers {
			w.updateMiddlewareAndJobTypes(wp.middlewares, map[string]*jobType{k: wp.jobTypes[k]})
		}
	}

	return wp
}

func (wp *WorkerPool) Job(name string, fn interface{}) *Pool {
	return wp.job(name, fn)
}

func (wp *WorkerPool) JobWithOptions(name string, jobOpts JobOptions, fn interface{}) *WorkerPool {
	wp.jobWithOptions(name, jobOpts, fn)

	for _, wg := range wp.WorkerGroup {
		for _, w := range wg.workers {
			w.updateMiddlewareAndJobTypes(wp.middlewares, map[string]*jobType{name: wp.jobTypes[name]})
		}
	}

	return wp
}

// Start starts the workers and associated processes.
func (wp *WorkerPool) Start() {
	concurrences := make(map[string]int)
	totalConcurrency := 0
	for k, wg := range wp.WorkerGroup {
		currCon := len(wg.workers)
		totalConcurrency += currCon
		concurrences[k] = currCon
		go wg.Start()
	}
	concurrencyMap, _ := json.Marshal(concurrences)

	wp.start(wp.kind, totalConcurrency, concurrencyMap, wp.workerIDs())
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
		go func(wkg *WorkerGroup) {
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
		go func(wkg *WorkerGroup) {
			wkg.Drain()
			wg.Done()
		}(wkg)
	}
	wg.Wait()

}

func (wp *WorkerPool) workerIDs() []string {
	l := 0
	for _, wkg := range wp.WorkerGroup {
		l += len(wkg.workers)
	}
	wids := make([]string, 0, l)
	for _, wkg := range wp.WorkerGroup {
		for _, w := range wkg.workers {
			wids = append(wids, w.workerID)
		}
	}
	sort.Strings(wids)
	return wids
}
