package pool

import (
	"encoding/json"
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/enqueue"
	"github.com/dhenisdj/scheduler/component/actors/execute/fetch"
	"github.com/dhenisdj/scheduler/component/actors/execute/group"
	"github.com/dhenisdj/scheduler/component/actors/execute/parse"
	"github.com/dhenisdj/scheduler/component/actors/task"
	context "github.com/dhenisdj/scheduler/component/common/context"
	"github.com/dhenisdj/scheduler/component/common/entities"
	"github.com/dhenisdj/scheduler/component/utils"
	"github.com/dhenisdj/scheduler/config"
	"github.com/gomodule/redigo/redis"
	"sort"
	"strings"
	"sync"
)

// FetcherPool represents a pool of workers. It forms the primary API of gocraft/work. WorkerPools provide the public API of gocraft/work. You can attach jobs and middlware to them. You can start and stop them. Based on their concurrency setting, they'll spin up N work goroutines.
type FetcherPool struct {
	*parse.Parser
	*Pool
	fetcherGroups map[string]*fetch.FetcherGroup
}

// NewFetcherPool creates a new work pool as per the NewWorkerPool function, but permits you to specify
// additional options such as sleep backoffs.
func NewFetcherPool(ctx *context.Context, configuration *entities.Configuration, pool *redis.Pool) *FetcherPool {
	if configuration == nil {
		panic("NewFetcherPool needs a non-nil *Configuration")
	}

	fp := &FetcherPool{
		Pool: newPool(
			configuration.Env,
			config.SchedulerNamespace,
			config.PoolKindFetcher,
			WithContext(ctx),
			WithBackoffs(config.SleepBackoffsInMilliseconds),
			WithRedis(pool),
		),
		Parser:        parse.NewParser(ctx, configuration.Spark),
		fetcherGroups: make(map[string]*fetch.FetcherGroup),
	}

	executorConfig := configuration.Spark.Executor
	amConfig := *configuration.AM
	credential := configuration.AM.Credential
	region := configuration.Region

	for _, executor := range *executorConfig {
		executorName := executor.Name
		business := executor.Business
		if !business.Valid() {
			panic("NewFetcherPool configuration file error for business")
		}
		session := executor.Session
		if !session.Valid() {
			panic("NewFetcherPool configuration file error for session")
		}
		bizConcurrency := business.Concurrency

		appKey := amConfig.Credential.Key
		appSecret := amConfig.Credential.Secret
		callBack := amConfig.Callback[region]

		for biz, _ := range bizConcurrency {

			fg := fetch.NewFetcherGroup(
				fp.env,
				fp.Namespace,
				fp.PoolID,
				appKey,
				appSecret,
				callBack,
				group.WithContext(ctx),
				group.WithRedis(pool),
			)

			if executorName == "SyncDistributionCommon" && biz == "ads_ops" {
				biz = "REFERRAL"
			}

			id := utils.MakeIdentifier()
			for i := 0; i < config.DefaultFetcherConcurrency; i++ {
				fetcherId := fmt.Sprintf("%s.%d", id, i)
				fetcher := fetch.NewFetcher(
					fg.Namespace,
					fetcherId,
					fp.PoolID,
					fg.GroupID,
					fetch.WithContext(ctx),
					fetch.WithEnv(fp.env),
					fetch.WithBiz(biz),
					fetch.WithExecutor(executorName),
					fetch.WithCallback(callBack),
					fetch.WithCredential(credential),
					fetch.WithBackoffs(fp.sleepBackoffs),
					fetch.WithParser(fp.Parser),
					fetch.WithEnqueuer(enqueue.NewEnqueuer(ctx, fg.Namespace, executorName, biz, pool)),
				)

				fg.Fetchers = append(fg.Fetchers, fetcher)
			}

			groupJobName := fmt.Sprintf("%s%s", executorName, strings.ToUpper(biz))

			//Build task types by executor and biz
			fp.registerJob(
				groupJobName,
				task.JobOptions{
					IsBatch:        session.IsBatch,
					SkipDead:       true,
					MaxConcurrency: uint(config.DefaultFetcherConcurrency),
					//Backoff:        task.DefaultBackoffCalculator, This is the default calculator if not set
				})

			fp.fetcherGroups[groupJobName] = fg
		}

	}
	ctx.If("%s Done init FetcherPool %s", fp.env, fp.PoolID)

	return fp
}

// RegisterJob adds a job type for 'name', along with additional options
// such as a task's priority, retry count, and whether to send dead jobs to the dead task queue or trash them.
func (fp *FetcherPool) registerJob(name string, jobOpts task.JobOptions) *FetcherPool {
	jobOpts = applyDefaultsAndValidate(jobOpts)

	jt := &task.JobType{
		Name:       name,
		JobOptions: jobOpts,
	}

	fp.jobTypes[name] = jt

	return fp
}

// Start starts the workers and associated processes.
func (fp *FetcherPool) Start(uri string) {
	concurrences := make(map[string]int)
	for k, fg := range fp.fetcherGroups {
		currCon := len(fg.Fetchers)
		concurrences[k] = currCon
		fg.Start(uri)
	}

	concurrencyMap, _ := json.Marshal(concurrences)

	fp.start(concurrencyMap, fp.fetcherIDs())
	fp.Pool.ctx.If("%s Fetcher Pool %s Started!", fp.env, fp.PoolID)
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
		go func(fg *fetch.FetcherGroup) {
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
		go func(fg *fetch.FetcherGroup) {
			fg.Drain()
			wg.Done()
		}(fg)
	}
	wg.Wait()
}

func (fp *FetcherPool) fetcherIDs() []string {
	l := 0
	for _, fg := range fp.fetcherGroups {
		l += len(fg.Fetchers)
	}
	fids := make([]string, 0, l)
	for _, fg := range fp.fetcherGroups {
		for _, f := range fg.Fetchers {
			fids = append(fids, f.FetcherID)
		}
	}
	sort.Strings(fids)
	return fids
}
