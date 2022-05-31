package pool

import (
	"fmt"
	"github.com/dhenisdj/scheduler/component/actors/enqueue"
	"github.com/dhenisdj/scheduler/component/actors/execute/fetch"
	"github.com/dhenisdj/scheduler/component/actors/execute/group"
	"github.com/dhenisdj/scheduler/component/actors/execute/parse"
	"github.com/dhenisdj/scheduler/component/actors/task"
	"github.com/dhenisdj/scheduler/component/context"
	"github.com/dhenisdj/scheduler/config"
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
func NewFetcherPool(ctx context.Context) *FetcherPool {
	configuration := ctx.CONF()
	pool := ctx.Redis()

	fp := &FetcherPool{
		Pool: newPool(
			configuration.NameSpace,
			config.PoolKindFetcher,
			WithContext(ctx),
			WithBackoffs(config.SleepBackoffsInSeconds),
			WithRedis(pool),
			WithConcurrency(make(map[string]int)),
		),
		Parser:        parse.NewParser(ctx),
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
		accounts := business.Account

		appKey := amConfig.Credential.Key
		appSecret := amConfig.Credential.Secret
		callBack := amConfig.Callback[region]

		for biz, _ := range bizConcurrency {

			account := accounts[biz]

			groupJobName := fmt.Sprintf("%s%s", executorName, strings.ToUpper(biz))

			fg := fetch.NewFetcherGroup(
				fp.env,
				fp.Namespace,
				fp.PoolID,
				groupJobName,
				appKey,
				appSecret,
				callBack,
				group.WithContext(ctx),
				group.WithRedis(pool),
			)

			if executorName == "SyncDistributionCommon" && biz == "ads_ops" {
				biz = "REFERRAL"
			}

			for i := 0; i < config.DefaultFetcherConcurrency; i++ {
				fetcherId := fmt.Sprintf("%s.%d", fg.GroupID, i)
				fetcher := fetch.NewFetcher(
					fg.Namespace,
					fetcherId,
					fp.PoolID,
					fg.GroupID,
					fetch.WithContext(ctx),
					fetch.WithAccount(account),
					fetch.WithEnv(fp.env),
					fetch.WithBiz(biz),
					fetch.WithExecutor(executorName),
					fetch.WithCallback(callBack),
					fetch.WithCredential(credential),
					fetch.WithBackoffs(fp.sleepBackoffs),
					fetch.WithParser(fp.Parser),
					fetch.WithEnqueuer(enqueue.NewEnqueuer(ctx, fg.Namespace, executorName, biz)),
				)

				fg.Fetchers = append(fg.Fetchers, fetcher)
			}

			//Build task types by executor and biz
			fp.concurrencyMap[groupJobName] = len(fg.Fetchers)
			fp.fetcherGroups[groupJobName] = fg
			fp.registerJob(
				groupJobName,
				task.JobOptions{
					IsBatch:        session.IsBatch,
					SkipDead:       false,
					MaxConcurrency: uint(config.DefaultFetcherConcurrency),
					//Backoff:        task.DefaultBackoffCalculator, This is the default calculator if not set
				})
		}

	}
	ctx.If("done init FetcherPool %s", fp.PoolID)

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

	for jobName, fg := range fp.fetcherGroups {
		if jobName == name {
			for _, f := range fg.Fetchers {
				f.UpdateJobTypes(jt)
			}
		}
	}

	return fp
}

// Start starts the workers and associated processes.
func (fp *FetcherPool) Start(uri string) {
	fp.start(fp.fetcherIDs())
	for _, fg := range fp.fetcherGroups {
		fg.Start(uri)
	}
	fp.Pool.ctx.If("fetcher pool %s started with %s!", fp.PoolID, uri)
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
