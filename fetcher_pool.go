package work

import (
	"encoding/json"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"reflect"
	"sort"
	"sync"
)

// FetcherPool represents a pool of workers. It forms the primary API of gocraft/work. WorkerPools provide the public API of gocraft/work. You can attach jobs and middlware to them. You can start and stop them. Based on their concurrency setting, they'll spin up N worker goroutines.
type FetcherPool struct {
	*Caller
	*Parser
	*Pool
	fetcherGroups map[string]*FetcherGroup
}

// NewFetcherPool creates a new worker pool. ctx should be a struct literal whose type will be used for middleware and handlers.
// concurrency specifies how many workers to spin up - each worker can process jobs concurrently.
func NewFetcherPool(ctx interface{}, configuration *Configuration, pool *redis.Pool) *FetcherPool {
	return NewFetcherPoolWithOptions(ctx, configuration, pool, PoolOptions{})
}

// NewFetcherPoolWithOptions creates a new worker pool as per the NewWorkerPool function, but permits you to specify
// additional options such as sleep backoffs.
func NewFetcherPoolWithOptions(ctx interface{}, configuration *Configuration, pool *redis.Pool, poolOpts PoolOptions) *FetcherPool {
	if configuration == nil {
		panic("NewFetcherPool needs a non-nil *Configuration")
	}

	ctxType := reflect.TypeOf(ctx)
	validateContextType(ctxType)
	fp := &FetcherPool{
		Pool:          newPoolWithOptions(ctx, FetcherPoolNamespace, PoolKindFetcher, IdentifierTypeFetcherPool, pool, poolOpts),
		Caller:        newCaller(),
		Parser:        newParser(configuration.Spark),
		fetcherGroups: make(map[string]*FetcherGroup),
	}

	executorConfig := configuration.Spark.Executor
	amConfig := *configuration.AM
	env := configuration.Env
	credential := configuration.AM.Credential
	region := configuration.Region

	for _, executor := range *executorConfig {
		executorName := executor.Name
		business := executor.Business
		if !business.validate() {
			panic("NewFetcherPool configuration file error for business")
		}
		bizConcurrency := business.Concurrency

		appKey := amConfig.Credential.Key
		appSecret := amConfig.Credential.Secret
		callBack := amConfig.Callback[region]

		fg := &FetcherGroup{
			fetcherGroupID: makeIdentifier(IdentifierTypeFetcherGroup),
			namespace:      fmt.Sprintf("%s:%s", fp.namespace, executorName),
			poolID:         fp.poolID,
			appKey:         appKey,
			appSecret:      appSecret,
			url:            callBack,
			env:            env,
			fetchers:       make([]*Fetcher, 0),
		}

		for biz, _ := range bizConcurrency {
			if executorName == "SyncDistributionCommon" && biz == "ads_ops" {
				biz = "REFERRAL"
			}

			for i := 0; i < DefaultFetcherConcurrency; i++ {
				id := fmt.Sprintf("fetcher.%s.%s.%d.%s", biz, executorName, i, makeSuffix())
				fetcher := newFetcher(
					fmt.Sprintf("%s:%s", fg.namespace, biz),
					id,
					fp.poolID,
					fg.fetcherGroupID,
					executorName,
					biz,
					callBack,
					env,
					credential,
					fp.sleepBackoffs,
					fp.contextType,
					fp.Caller,
					fp.Parser,
					fp.pool,
				)
				fg.fetchers = append(fg.fetchers, fetcher)
			}
		}
		fp.fetcherGroups[executorName] = fg

		isBatch := false
		if executor.Type == JobTypeBatch {
			isBatch = true
		}

		//Build job types by executor
		fp.JobWithOptions(
			executorName,
			JobOptions{
				IsBatch:        isBatch,
				SkipDead:       true,
				MaxConcurrency: uint(len(fp.fetcherGroups[executorName].fetchers)),
				Backoff:        defaultBackoffCalculator,
			},
			epsilonHandler)
	}

	return fp
}

// Middleware appends the specified function to the middleware chain. The fn can take one of these forms:
// (*ContextType).func(*Job, NextMiddlewareFunc) error, (ContextType matches the type of ctx specified when creating a pool)
// func(*Job, NextMiddlewareFunc) error, for the generic middleware format.
func (fp *FetcherPool) Middleware(fn interface{}) *FetcherPool {
	fp.middleware(fn)

	for k, fg := range fp.fetcherGroups {
		for _, f := range fg.fetchers {
			f.updateMiddlewareAndJobTypes(fp.middlewares, map[string]*jobType{k: fp.jobTypes[k]})
		}
	}

	return fp
}

// JobWithOptions adds a handler for 'name' jobs as per the Job function, but permits you specify additional options
// such as a job's priority, retry count, and whether to send dead jobs to the dead job queue or trash them.
func (fp *FetcherPool) JobWithOptions(name string, jobOpts JobOptions, fn interface{}) *FetcherPool {
	fp.jobWithOptions(name, jobOpts, fn)

	for _, fg := range fp.fetcherGroups {
		for _, f := range fg.fetchers {
			f.updateMiddlewareAndJobTypes(fp.middlewares, map[string]*jobType{name: fp.jobTypes[name]})
		}
	}

	return fp
}

// Start starts the workers and associated processes.
func (fp *FetcherPool) Start(uri string) {
	concurrences := make(map[string]int)
	totalConcurrency := 0
	for k, fg := range fp.fetcherGroups {
		currCon := len(fg.fetchers)
		totalConcurrency += currCon
		concurrences[k] = currCon
		fg.Start(uri)
	}

	concurrencyMap, _ := json.Marshal(concurrences)

	fp.start(fp.kind, totalConcurrency, concurrencyMap, fp.fetcherIDs())
	fp.startRequeuers(fp.jobTypes, fp.pool)
}

// Stop stops the workers and associated processes.
func (fp *FetcherPool) Stop() {
	if !fp.started {
		return
	}
	fp.started = false

	wg := sync.WaitGroup{}
	for _, fg := range fp.fetcherGroups {
		wg.Add(1)
		go func(fg *FetcherGroup) {
			fg.Stop()
			wg.Done()
		}(fg)
	}
	wg.Wait()
	fp.stop()
}

// Drain drains all jobs in the queue before returning. Note that if jobs are added faster than we can process them, this function wouldn't return.
func (fp *FetcherPool) Drain() {
	wg := sync.WaitGroup{}
	for _, fg := range fp.fetcherGroups {
		wg.Add(1)
		go func(fg *FetcherGroup) {
			fg.Drain()
			wg.Done()
		}(fg)
	}
	wg.Wait()
}

func (fp *FetcherPool) fetcherIDs() []string {
	l := 0
	for _, fg := range fp.fetcherGroups {
		l += len(fg.fetchers)
	}
	fids := make([]string, 0, l)
	for _, fg := range fp.fetcherGroups {
		for _, f := range fg.fetchers {
			fids = append(fids, f.fetcherID)
		}
	}
	sort.Strings(fids)
	return fids
}
